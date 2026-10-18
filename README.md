# traceability-service — VetChain

Servicio de trazabilidad. Verifica la autenticidad de un serial o lote y reconstruye su
**cadena de custodia** (fabricado → transferido → recibido → dispensado / retirado).

## Responsabilidad en la arquitectura

Es el **libro mayor inmutable** de la historia de cada unidad. No lo alimentan llamadas directas: su
tabla `custody_chain` se construye **únicamente a partir de los eventos de RabbitMQ**, lo que lo
desacopla de los servicios que producen los hechos.

- Responde "¿este serial es auténtico?" a dispensing y a la administración.
- Expone la cadena de custodia por serial y por lote.
- Sirve de apoyo a recall (para saber a qué entidades llegó un lote).

## Tecnologías

| Componente | Versión |
|---|---|
| Node.js | ≥ 18 |
| Express | ^5.2.1 |
| pg | ^8.16.3 |
| amqplib (RabbitMQ) | ^0.10.9 |
| dotenv | ^18.0.1 |

## Instalación y ejecución

**Método oficial (recomendado): Docker Compose**, desde la raíz del proyecto:

```bash
docker compose up -d --build --wait
```

**Ejecución directa** (requiere PostgreSQL y RabbitMQ):

```bash
npm install
node index.js        # o: npm start
```

El servicio escucha en `PORT` (3004 por defecto). Al conectar al bus registra en el log
`escuchando eventos en RabbitMQ`.

## Endpoints principales

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/` , `/health` | Identificación y estado. |
| GET | `/verify/:serial` | Verifica la autenticidad de un serial. |
| GET | `/verify-batch/:batch_id` | Verifica un lote completo. |
| GET | `/custody-chain/batch/:batch_id` | Cadena de custodia de un lote. |
| GET | `/custody-chain/:serial` | Cadena de custodia de un serial. |

## Variables de entorno

| Variable | Defecto | Descripción |
|---|---|---|
| `PORT` | `3004` | Puerto de escucha. |
| `DATABASE_URL` | — | Conexión a PostgreSQL. |
| `DB_NAME` | — | Base de datos a usar dentro de la misma instancia. |
| `DB_POOL_MAX` | `10` | Tamaño máximo del pool. |
| `RABBITMQ_URL` | `amqp://localhost:5672` | Conexión AMQP. |
| `JWT_SECRET` | — | Verificación del token. |
| `AUTH_SERVICE_URL` | `http://localhost:3001` | Validación de sesión/ámbito. |

## Base de datos

- `custody_chain` (`id`, `serial_number`, `event_type`, `event_data` JSONB, `timestamp`)
  + índices `idx_chain_serial`, `idx_chain_batch`.

## Reglas de negocio

- La cadena de custodia se alimenta **solo** de eventos (`batch.created` → `fabricado`,
  `custody.transferred` → `transferido`, `unit.dispensed`, `shipment.delivered`, `recall.initiated`).
- **Sin bus de eventos activo**, `/verify/:serial` devuelve `{ authentic: false, status: "no_registrado" }`.
  Por eso el bus (RabbitMQ) es imprescindible para que la dispensación funcione de extremo a extremo.
- `isBatchRecalled` considera un lote retirado si existe un evento `BATCH:<id>` con tipo `retirado`.
- La inserción **deduplica** el evento `fabricado` para no duplicar el origen de la unidad.

## Eventos

- **Consume** (`vetchain.events`): `batch.created`, `custody.transferred`, `unit.dispensed`,
  `shipment.delivered`, `recall.initiated`.
- **Publica**: ninguno.
