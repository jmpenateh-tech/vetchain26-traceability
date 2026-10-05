import pg from "pg";
import amqp from "amqplib";

const { Pool } = pg;

// Las columnas DATE (OID 1082) se devuelven como texto "YYYY-MM-DD".
// Por defecto pg las convierte a Date con la zona horaria local y, según el
// servidor, la fecha podía aparecer corrida un día en el JSON.
pg.types.setTypeParser(1082, (v) => v);

if (!process.env.DATABASE_URL) {
  console.warn("[db] DATABASE_URL no está definida (revisa el archivo .env del servicio)");
}

// DB_NAME (opcional): usa otra base dentro de la MISMA instancia de PostgreSQL.
// En Render (plan gratis) todos los servicios reciben la misma DATABASE_URL de la
// instancia y DB_NAME elige su base (manufacturing_db, ...). Si la base no existe,
// initDb() la crea sola. En Docker Compose no se usa: DATABASE_URL ya trae la base.
function resolveDbUrl() {
  const raw = process.env.DATABASE_URL;
  const name = (process.env.DB_NAME || "").trim();
  if (!raw || !name) return raw;
  const u = new URL(raw);
  u.pathname = "/" + encodeURIComponent(name);
  return u.toString();
}
const DB_URL = resolveDbUrl();

// DB_POOL_MAX (opcional): máximo de conexiones por servicio. En el plan gratis de
// Render conviene un valor bajo porque las 9 bases comparten una instancia pequeña.
export const pool = new Pool({ connectionString: DB_URL, max: Number(process.env.DB_POOL_MAX) || 10 });
pool.on("error", (e) => console.error("[db] error en una conexión inactiva:", e.message));

export const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost:5672";
export const EXCHANGE = "vetchain.events";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// PostgreSQL
// ---------------------------------------------------------------------------
const TRANSIENT_DB_ERRORS = new Set([
  "ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "ENOTFOUND", "EAI_AGAIN",
  "57P03", // cannot_connect_now (el servidor está arrancando)
  "08000", "08001", "08003", "08006",
]);

// Crea la base de datos del servicio si no existe (por ejemplo, cuando el
// volumen de Postgres se creó antes de que init.sql tuviera esa base).
async function ensureDatabase() {
  const dbName = decodeURIComponent(new URL(DB_URL).pathname.slice(1));
  // Con DB_NAME nos conectamos a la base original de DATABASE_URL (que sí existe y a la
  // que el usuario de Render tiene acceso); sin DB_NAME, a "postgres" como siempre.
  const url = new URL(process.env.DATABASE_URL);
  if (!(process.env.DB_NAME || "").trim()) url.pathname = "/postgres";
  const admin = new pg.Client({ connectionString: url.toString() });
  await admin.connect();
  try {
    const r = await admin.query("select 1 from pg_database where datname=$1", [dbName]);
    if (!r.rowCount) {
      await admin.query(`CREATE DATABASE "${dbName.replace(/"/g, '""')}"`);
      console.log(`[db] base de datos "${dbName}" creada`);
    }
  } finally {
    await admin.end();
  }
}

// Ejecuta el DDL inicial. Espera a que PostgreSQL esté disponible en lugar de
// caerse si el servicio arranca antes que la base de datos.
export async function initDb(sql, { retries = 30, delayMs = 2000 } = {}) {
  let triedCreate = false;
  for (let attempt = 1; ; attempt++) {
    try {
      await pool.query(sql);
      return;
    } catch (e) {
      if (e.code === "3D000" && !triedCreate) {
        triedCreate = true;
        try {
          await ensureDatabase();
          continue;
        } catch (e2) {
          console.error("[db] la base de datos no existe y no se pudo crear:", e2.message);
          throw e;
        }
      }
      const transient =
        TRANSIENT_DB_ERRORS.has(e.code) ||
        /starting up|shutting down|terminating connection/i.test(e.message || "");
      if (!transient || attempt >= retries) throw e;
      console.warn(`[db] PostgreSQL no disponible (${e.code || e.message}). Reintento ${attempt}/${retries}...`);
      await sleep(delayMs);
    }
  }
}

// ---------------------------------------------------------------------------
// RabbitMQ
// ---------------------------------------------------------------------------
let publishChannelPromise = null;

function getPublishChannel() {
  if (!publishChannelPromise) {
    publishChannelPromise = (async () => {
      const conn = await amqp.connect(RABBITMQ_URL);
      const reset = () => { publishChannelPromise = null; };
      conn.on("error", (e) => console.error("[rabbitmq] error de conexión:", e.message));
      conn.on("close", reset);
      const ch = await conn.createConfirmChannel();
      ch.on("error", (e) => console.error("[rabbitmq] error de canal:", e.message));
      ch.on("close", reset);
      await ch.assertExchange(EXCHANGE, "fanout", { durable: true });
      return ch;
    })().catch((e) => {
      publishChannelPromise = null;
      throw e;
    });
  }
  return publishChannelPromise;
}

// Publica un evento. Nunca lanza: si RabbitMQ no responde se registra el error
// y la petición HTTP (cuyo dato ya se guardó en la base) no se convierte en 500.
export async function publish(event, payload) {
  try {
    const ch = await getPublishChannel();
    ch.publish(
      EXCHANGE,
      "",
      Buffer.from(JSON.stringify({ event, timestamp: new Date().toISOString(), payload })),
      { persistent: true }
    );
    await ch.waitForConfirms();
    return true;
  } catch (e) {
    console.error(`[rabbitmq] no se pudo publicar "${event}":`, e.message);
    return false;
  }
}

// Escucha los eventos del exchange. Si RabbitMQ aún no está listo (o se cae)
// reintenta con espera creciente en lugar de quedarse sordo para siempre.
export async function consume(queue, handler) {
  let delay = 2000;
  for (;;) {
    let conn;
    try {
      conn = await amqp.connect(RABBITMQ_URL);
      conn.on("error", (e) => console.error(`[${queue}] error de conexión RabbitMQ:`, e.message));
      const ch = await conn.createChannel();
      ch.on("error", (e) => console.error(`[${queue}] error de canal RabbitMQ:`, e.message));
      await ch.assertExchange(EXCHANGE, "fanout", { durable: true });
      await ch.assertQueue(queue, { durable: true });
      await ch.bindQueue(queue, EXCHANGE, "");
      await ch.prefetch(10);
      await ch.consume(queue, async (msg) => {
        if (!msg) return;
        try {
          await handler(JSON.parse(msg.content.toString()));
          ch.ack(msg);
        } catch (e) {
          // Un reintento; si vuelve a fallar se descarta para no bloquear la cola.
          console.error(`[${queue}] error procesando evento`, e);
          ch.nack(msg, false, !msg.fields.redelivered);
        }
      });
      conn.on("close", () => {
        console.warn(`[${queue}] conexión con RabbitMQ cerrada. Reconectando...`);
        setTimeout(() => consume(queue, handler), 2000);
      });
      console.log(`[${queue}] escuchando eventos en RabbitMQ`);
      return;
    } catch (e) {
      console.error(`[${queue}] RabbitMQ no disponible (${e.message}). Reintento en ${delay / 1000}s...`);
      if (conn) conn.close().catch(() => {});
      await sleep(delay);
      delay = Math.min(delay * 2, 15000);
    }
  }
}

// ---------------------------------------------------------------------------
// Utilidades HTTP
// ---------------------------------------------------------------------------
export function jsonError(res, status, message) {
  return res.status(status).json({ error: message });
}

// Responde a un error inesperado dentro de un handler (formato inválido => 400).
export function fail(res, e, message) {
  if (["22P02", "22007", "22008", "22003"].includes(e?.code)) {
    return res.status(400).json({ error: "Valor o identificador con formato inválido" });
  }
  console.error(e);
  return res.status(500).json({ error: message });
}

// En Express 5 req.body es undefined cuando la petición no trae cuerpo JSON.
export function bodyDefault(req, _res, next) {
  if (req.body === undefined) req.body = {};
  next();
}

// Debe registrarse DESPUÉS de todas las rutas: 404 y errores siempre en JSON.
export function installErrorHandlers(app) {
  app.use((_req, res) => res.status(404).json({ error: "Ruta no encontrada" }));
  app.use((err, _req, res, next) => {
    if (res.headersSent) return next(err);
    if (err?.type === "entity.parse.failed") {
      return res.status(400).json({ error: "JSON inválido en el cuerpo de la petición" });
    }
    if (["22P02", "22007", "22008", "22003"].includes(err?.code)) {
      return res.status(400).json({ error: "Valor o identificador con formato inválido" });
    }
    console.error(err);
    res.status(500).json({ error: "Error interno del servidor" });
  });
}
