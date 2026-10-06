import "dotenv/config";
import express from "express";
import { pool, initDb, consume, bodyDefault, installErrorHandlers, authenticate, auditTrail } from "./common.js";

const app = express();
const PORT = process.env.PORT || 3004;

app.disable("x-powered-by");
app.use(express.json());
app.use(bodyDefault);

await initDb(`
CREATE TABLE IF NOT EXISTS custody_chain(
  id BIGSERIAL PRIMARY KEY,
  serial_number TEXT NOT NULL,
  event_type TEXT NOT NULL,
  event_data JSONB,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS idx_chain_serial ON custody_chain(serial_number);
CREATE INDEX IF NOT EXISTS idx_chain_batch ON custody_chain((event_data->'batch'->>'id'));`);

// Todos los eventos de un lote: los del propio lote (fabricación, recall) y los de
// cada una de sus unidades (transferencias, dispensaciones), aunque esos eventos no
// mencionen el lote. Antes solo se encontraban los eventos "fabricado".
const BATCH_EVENTS_SQL = `
  select * from custody_chain
  where event_data->'batch'->>'id' = $1
     or event_data->>'batch_id' = $1
     or serial_number in (select serial_number from custody_chain where event_data->'batch'->>'id' = $1)`;

async function isBatchRecalled(batchId) {
  const r = await pool.query(
    "select 1 from custody_chain where serial_number=$1 and event_type='retirado' limit 1", [`BATCH:${batchId}`]);
  return r.rowCount > 0;
}

async function add(serial, event, data) {
  if (!serial) return;
  if (event === "fabricado") {
    // batch.created puede llegar repetido (p. ej. al regenerar seriales): no duplicar.
    await pool.query(
      `insert into custody_chain(serial_number,event_type,event_data)
       select $1,$2,$3 where not exists
         (select 1 from custody_chain where serial_number=$1 and event_type='fabricado')`,
      [serial, event, data || {}]);
    return;
  }
  await pool.query(
    "insert into custody_chain(serial_number,event_type,event_data) values($1,$2,$3)",
    [serial, event, data || {}]);
}

app.get("/", (_, res) => res.json({ service: "traceability-service", status: "ok" }));
app.get("/health", async (_, res) => {
  try { await pool.query("select 1"); res.json({ status: "ok" }); }
  catch { res.status(503).json({ status: "down" }); }
});

// 2.7: todas las rutas (salvo / y /health) exigen una sesión válida (C1).
// 2.8: registro de auditoría (va antes de authenticate para anotar también los 401).
app.use(auditTrail("traceability", []));
app.use(authenticate);

app.get("/verify/:serial", async (req, res) => {
  const serial = req.params.serial;
  const r = await pool.query(
    "select * from custody_chain where serial_number=$1 order by timestamp desc, id desc", [serial]);
  if (!r.rowCount) return res.json({ serial, authentic: false, status: "no_registrado" });
  // 2.7 (C3): una unidad es auténtica SOLO si tiene evento de fabricación. Antes
  // bastaba con cualquier evento, así que un serial inventado que alguien
  // "transfería" aparecía como auténtico.
  if (!r.rows.some((e) => e.event_type === "fabricado")) {
    return res.json({ serial, authentic: false, status: "no_registrado", suspicious: true, events: r.rows });
  }

  let status = r.rows[0].event_type;
  const batchId = r.rows.find((e) => e.event_data?.batch?.id)?.event_data.batch.id || null;
  const recalled = batchId ? await isBatchRecalled(batchId) : false;
  // Una unidad ya dispensada conserva ese estado; el resto de un lote retirado queda "retirado".
  if (recalled && !r.rows.some((e) => e.event_type === "dispensado")) status = "retirado";

  res.json({ serial, authentic: true, status, recalled, batch_id: batchId, events: r.rows });
});

app.get("/verify-batch/:batch_id", async (req, res) => {
  const id = req.params.batch_id;
  const r = await pool.query(`${BATCH_EVENTS_SQL} order by timestamp desc, id desc`, [id]);
  const recalled = r.rows.some((e) => e.event_type === "retirado");
  const summary = {};
  for (const e of r.rows) {
    if (String(e.serial_number).startsWith("BATCH:")) continue; // marcadores del lote, no unidades
    summary[e.event_type] = (summary[e.event_type] || 0) + 1;
  }
  res.json({
    batch_id: id,
    authentic: r.rows.some((e) => e.event_type === "fabricado"),
    status: r.rowCount ? (recalled ? "retirado" : r.rows[0].event_type) : "no_registrado",
    recalled,
    summary,
    events: r.rows,
  });
});

app.get("/custody-chain/batch/:batch_id", async (req, res) => {
  const r = await pool.query(`${BATCH_EVENTS_SQL} order by timestamp, id`, [req.params.batch_id]);
  res.json(r.rows);
});

app.get("/custody-chain/:serial", async (req, res) => {
  const r = await pool.query(
    "select * from custody_chain where serial_number=$1 order by timestamp, id", [req.params.serial]);
  res.json({ serial: req.params.serial, events: r.rows });
});

consume("traceability-service", async (msg) => {
  if (msg.event === "batch.created") {
    const b = msg.payload.batch;
    const serials = msg.payload.serial_numbers || [];
    if (serials.length) for (const serial of serials) await add(serial, "fabricado", { batch: b });
    else await add(`BATCH:${b.id}`, "fabricado", { batch: b });
  }
  if (msg.event === "custody.transferred") {
    const t = msg.payload.transfer;
    await add(t.serial_number, "transferido", { transfer: t, batch_id: msg.payload.batch_id });
  }
  if (msg.event === "unit.dispensed") {
    const d = msg.payload.dispensation;
    await add(d.serial_number, "dispensado", { dispensation: d });
  }
  if (msg.event === "shipment.delivered") {
    const sh = msg.payload.shipment;
    await add(`BATCH:${msg.payload.batch_id}`, "recibido", { shipment: sh, batch_id: msg.payload.batch_id });
  }
  if (msg.event === "recall.initiated") {
    await add(`BATCH:${msg.payload.batch_id}`, "retirado", msg.payload);
  }
});

installErrorHandlers(app);
app.listen(PORT, () => console.log(`Traceability Service http://localhost:${PORT}`));
