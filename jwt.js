// JWT HS256 mínimo con el módulo "crypto" de Node (sin dependencias extra).
// El mismo archivo existe en api-gateway/jwt.js y auth-service/jwt.js, y su lógica
// de verificación está copiada en el common.js de cada microservicio (2.7).
// Todos usan JWT_SECRET para firmar (auth-service) y verificar (el resto).
import crypto from "crypto";

const b64url = (buf) => Buffer.from(buf).toString("base64url");
const HEADER = b64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));

// Tipo de token. Desde la 2.7 solo los tokens "access" dan acceso a la API; un
// token sin "typ" (versiones anteriores) o de otro tipo se rechaza.
export const TOKEN_TYPE = "access";

export function resolveSecret(serviceName) {
  const secret = (process.env.JWT_SECRET || "").trim();
  if (secret) return secret;
  if (process.env.NODE_ENV === "production") {
    console.error(`[${serviceName}] JWT_SECRET es obligatorio en producción (mismo valor en todos los servicios).`);
    process.exit(1);
  }
  console.warn(`[${serviceName}] JWT_SECRET no definido: se usa un valor de DESARROLLO. No lo uses en producción.`);
  return "vetchain-dev-secret-cambiar";
}

export function signToken(payload, secret, ttlSeconds = 8 * 3600) {
  const now = Math.floor(Date.now() / 1000);
  const body = b64url(JSON.stringify({ ...payload, typ: TOKEN_TYPE, iat: now, exp: now + ttlSeconds }));
  const sig = crypto.createHmac("sha256", secret).update(`${HEADER}.${body}`).digest("base64url");
  return `${HEADER}.${body}.${sig}`;
}

// Devuelve el payload o null si el token es inválido, expiró o no es de acceso.
export function verifyToken(token, secret) {
  if (typeof token !== "string") return null;
  const parts = token.split(".");
  if (parts.length !== 3 || parts[0] !== HEADER) return null;
  const expected = crypto.createHmac("sha256", secret).update(`${parts[0]}.${parts[1]}`).digest();
  let given;
  try { given = Buffer.from(parts[2], "base64url"); } catch { return null; }
  if (given.length !== expected.length || !crypto.timingSafeEqual(given, expected)) return null;
  let payload;
  try { payload = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8")); } catch { return null; }
  if (!payload || typeof payload.exp !== "number" || payload.exp < Math.floor(Date.now() / 1000)) return null;
  if (payload.typ !== TOKEN_TYPE || payload.sub === undefined || typeof payload.role !== "string") return null;
  return payload;
}

export function bearer(req) {
  const h = req.headers.authorization || "";
  return h.startsWith("Bearer ") ? h.slice(7).trim() : null;
}
