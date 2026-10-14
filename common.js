import pg from "pg";
import amqp from "amqplib";
import crypto from "crypto";
import { resolveSecret, verifyToken, bearer } from "./jwt.js";

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

// ---------------------------------------------------------------------------
// Seguridad (VetChain 2.7)
//
// Antes (2.6) los microservicios confiaban en que el gateway ya había validado
// el token: como en Render cada servicio tiene URL pública, cualquiera podía
// llamarlos directamente sin sesión (vulnerabilidad C1). Ahora cada servicio:
//   1. verifica la firma y la expiración del JWT (mismo JWT_SECRET que el gateway);
//   2. confirma con auth-service que la sesión sigue vigente (cuenta activa,
//      mismo rol y misma versión de sesión; vulnerabilidad A3), con caché corta;
//   3. toma la identidad (nombre y rol) del token, nunca del cuerpo (A1).
// ---------------------------------------------------------------------------
export const withScheme = (url) => (/^https?:\/\//.test(url) ? url : `http://${url}`);
const JWT_SECRET = resolveSecret("servicio");
export const AUTH_URL = withScheme(process.env.AUTH_SERVICE_URL || "localhost:3001");
const SESSION_CACHE_MS = Number(process.env.SESSION_CACHE_MS) || 30000;
const SESSION_TIMEOUT_MS = Number(process.env.SESSION_TIMEOUT_MS) || 15000;
const sessionCache = new Map();

// Pregunta a auth-service si la sesión sigue vigente. Las respuestas definitivas
// (válida / revocada) se guardan SESSION_CACHE_MS; un fallo de red no se guarda.
export async function checkSession(token) {
  const key = token.slice(-43);
  const hit = sessionCache.get(key);
  if (hit && hit.until > Date.now()) return hit.result;
  let result;
  try {
    const r = await fetch(`${AUTH_URL}/session`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(SESSION_TIMEOUT_MS),
    });
    if (r.ok) result = { ok: true };
    else if (r.status === 401 || r.status === 403) {
      const d = await r.json().catch(() => ({}));
      result = { ok: false, status: 401, error: d.error || "Tu sesión ya no es válida. Inicia sesión de nuevo.", code: d.code || "session_revoked" };
    } else throw new Error(`HTTP ${r.status}`);
  } catch (e) {
    console.error("[auth] no se pudo validar la sesión:", e.message);
    return { ok: false, status: 503, error: "No se pudo validar la sesión. Intenta de nuevo en unos segundos." };
  }
  if (sessionCache.size > 5000) sessionCache.clear();
  sessionCache.set(key, { result, until: Date.now() + SESSION_CACHE_MS });
  return result;
}

// Middleware: todas las rutas exigen sesión salvo GET / y GET /health.
export async function authenticate(req, res, next) {
  if (req.method === "GET" && (req.path === "/" || req.path === "/health")) return next();
  const token = bearer(req);
  const payload = verifyToken(token, JWT_SECRET);
  if (!payload) return jsonError(res, 401, "Sesión no válida o expirada. Inicia sesión de nuevo.");
  const s = await checkSession(token);
  if (!s.ok) return res.status(s.status).json({ error: s.error, ...(s.code ? { code: s.code } : {}) });
  req.auth = { id: payload.sub, role: payload.role, name: String(payload.name || "").trim() };
  req.token = token;
  next();
}

// Restringe una ruta a ciertos roles (defensa en profundidad: el gateway también lo hace).
export const requireRole = (...roles) => (req, res, next) =>
  roles.includes(req.auth?.role) ? next() : jsonError(res, 403, "Tu rol no tiene permiso para esta acción");

// 2.10: jerarquía de administración.
//   isMaster: administrador general (ve toda la plataforma).
//   isAdmin:  cualquier rol de administración (admin o master-admin). Un "admin" solo
//             puede ver los datos de SU ámbito: usa getScope() antes de devolver nada.
export const ADMIN_ROLES = ["master-admin", "admin"];
export const isMaster = (req) => req.auth?.role === "master-admin";
export const isAdmin = (req) => ADMIN_ROLES.includes(req.auth?.role);

// Ámbito de datos del que llama, calculado por auth-service (GET /scope) con datos de la
// base, nunca con lo que diga el cliente. Opcionalmente acotado a un administrador
// (admin_id) o a un usuario (user_id): auth-service responde 403 si el que llama no
// tiene acceso a ese ámbito. Caché corta por token+consulta.
export class ScopeError extends Error { constructor(status, message) { super(message); this.status = status; } }
const SCOPE_CACHE_MS = Number(process.env.SCOPE_CACHE_MS) || 10000;
const scopeCache = new Map();
export async function getScope(req, { adminId, userId } = {}) {
  const qs = new URLSearchParams();
  if (adminId !== undefined && adminId !== null && adminId !== "") qs.set("admin_id", String(adminId));
  if (userId !== undefined && userId !== null && userId !== "") qs.set("user_id", String(userId));
  const key = `${req.token.slice(-43)}?${qs}`;
  const hit = scopeCache.get(key);
  if (hit && hit.until > Date.now()) return hit.scope;
  let r;
  try {
    r = await fetch(`${AUTH_URL}/scope${qs.size ? `?${qs}` : ""}`, { headers: { Authorization: `Bearer ${req.token}` }, signal: AbortSignal.timeout(SESSION_TIMEOUT_MS) });
  } catch (e) {
    console.error("[scope] auth-service no respondió:", e.message);
    throw new ScopeError(503, "No se pudo comprobar tu ámbito de acceso. Intenta de nuevo en unos segundos.");
  }
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new ScopeError(r.status === 401 ? 401 : r.status === 404 ? 404 : r.status === 403 ? 403 : 503, data.error || "No tienes acceso a esa información");
  const names = new Set((data.users || []).map((u) => normName(u.name)).filter(Boolean));
  const scope = { ...data, names, has: (name) => data.all || names.has(normName(name)) };
  if (scopeCache.size > 2000) scopeCache.clear();
  scopeCache.set(key, { scope, until: Date.now() + SCOPE_CACHE_MS });
  return scope;
}
const normName = (v) => String(v ?? "").trim().toLowerCase();
// Compara nombres de entidad sin distinguir mayúsculas ni espacios sobrantes.
export const sameEntity = (a, b) => String(a ?? "").trim().toLowerCase() === String(b ?? "").trim().toLowerCase();

// Llamada a otro microservicio reenviando el token del usuario. Devuelve
// { status, data } o lanza si no hay respuesta.
export async function callService(base, path, req, { method = "GET", body, timeout = 8000 } = {}) {
  const headers = { Authorization: `Bearer ${req.token}` };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const r = await fetch(`${base}${path}`, {
    method, headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(timeout),
  });
  const text = await r.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = { message: text }; }
  return { status: r.status, ok: r.ok, data };
}

// ---------------------------------------------------------------------------
// Auditoría (VetChain 2.8)
// Cada servicio envía a auth-service (POST /internal/audit) los eventos relevantes,
// firmados con HMAC-SHA256 (JWT_SECRET). auditTrail() se registra ANTES de
// authenticate y, al terminar cada petición, anota:
//   - 401: llamada sin sesión válida (p. ej. directa al servicio, saltándose el gateway);
//   - 403: acceso denegado (rol o propiedad del recurso);
//   - escrituras correctas que coinciden con una regla (lote creado, envío, recall...);
//   - escrituras rechazadas (400/409), p. ej. un serial inventado.
// Envío "dispara y olvida": si auth-service no responde, la operación sigue igual.
// ---------------------------------------------------------------------------
const AUDIT_MAX_PER_MIN = Number(process.env.AUDIT_MAX_PER_MIN) || 300;
let auditWindow = { start: Date.now(), n: 0 };

export function clientIp(req) {
  const xff = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
  return (xff || req.socket?.remoteAddress || "").replace(/^::ffff:/, "") || null;
}

export function sendAudit(ev) {
  if (Date.now() - auditWindow.start > 60_000) auditWindow = { start: Date.now(), n: 0 };
  if (++auditWindow.n > AUDIT_MAX_PER_MIN) return; // protege la base ante una avalancha
  const body = JSON.stringify(ev);
  const ts = String(Date.now());
  const sig = crypto.createHmac("sha256", JWT_SECRET).update(`${ts}.${body}`).digest("hex");
  fetch(`${AUTH_URL}/internal/audit`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Audit-Ts": ts, "X-Audit-Signature": sig },
    body, signal: AbortSignal.timeout(5000),
  }).catch((e) => console.error("[audit] no se pudo enviar el evento:", e.message));
}

// rules: [[método, /ruta/, acción, (cuerpo, respuesta) => ({ target, message, severity? })], ...]
export function auditTrail(service, rules = []) {
  return (req, res, next) => {
    let payload;
    const json = res.json.bind(res);
    res.json = (d) => { payload = d; return json(d); };
    res.on("finish", () => {
      const status = res.statusCode;
      const base = {
        service, ip: clientIp(req), method: req.method, path: req.originalUrl.split("?")[0],
        actor_id: req.auth?.id ?? null, actor_name: req.auth?.name || null, actor_role: req.auth?.role || null,
      };
      const write = !["GET", "HEAD", "OPTIONS"].includes(req.method);
      const rule = rules.find(([m, re]) => m === req.method && re.test(req.path));
      const error = payload?.error ? String(payload.error) : "";
      if (status === 401) {
        if (payload?.code === "session_revoked" || payload?.code === "account_disabled") return; // ya lo anota auth-service
        sendAudit({ ...base, action: "access.unauthenticated", outcome: "denied", severity: write ? "critical" : "warning",
          message: `Petición sin sesión válida a ${service}${req.headers.authorization ? " (token no válido)" : " (sin token)"}` });
      } else if (status === 403) {
        sendAudit({ ...base, action: "access.denied", outcome: "denied", severity: "warning", message: error || "Acceso denegado" });
      } else if (rule && status >= 200 && status < 300) {
        const extra = rule[3] ? rule[3](req.body || {}, payload || {}) : {};
        sendAudit({ ...base, action: rule[2], outcome: "ok", severity: extra.severity || "info", target: extra.target, message: extra.message });
      } else if (write && rule && (status === 400 || status === 409)) {
        const suspicious = /falsific|no pertenecen|no fue fabricada|custodia/i.test(error);
        sendAudit({ ...base, action: `${rule[2].split(".")[0]}.rejected`, outcome: "rejected", severity: suspicious ? "critical" : "warning",
          message: error || "Operación rechazada", target: rule[3] ? rule[3](req.body || {}, {}).target : undefined });
      }
    });
    next();
  };
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
