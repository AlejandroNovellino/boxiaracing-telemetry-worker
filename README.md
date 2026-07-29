# boxiaracing-telemetry-worker

Worker de telemetría para Boxia Racing. Consume eventos de Kafka, transforma su
contenido a JSON y publica el resultado en otro tópico para que Tinybird lo
ingiera. Los eventos que no puedan transformarse se publican en una dead-letter
queue (DLQ).

> [!IMPORTANT]
> El transformador actual, `JSONPassthrough`, es provisional: valida que la
> entrada sea JSON y la reenvía sin modificarla. No intenta detectar ni
> decodificar Base64. Debe sustituirse cuando se incorpore el documento que
> define las estructuras binarias reales.

## Flujo

```text
Kafka input ──> Transformer ──> Kafka output ──> Tinybird
                    │
                    └────────> Kafka DLQ
```

El consumer utiliza commits manuales. Un offset solo se confirma después de
que el resultado o su registro de DLQ hayan sido reconocidos por Kafka. La
entrega es **al menos una vez**, por lo que los consumidores posteriores deben
tolerar duplicados.

## Configuración

| Variable | Requerida | Valor predeterminado | Descripción |
| --- | --- | --- | --- |
| `KAFKA_BROKERS` | Sí | — | Brokers separados por comas. |
| `KAFKA_INPUT_TOPIC` | Sí | — | Tópico de entrada. |
| `KAFKA_OUTPUT_TOPIC` | Sí | — | Tópico JSON de salida. |
| `KAFKA_DLQ_TOPIC` | Sí | — | Tópico de mensajes rechazados. |
| `KAFKA_CONSUMER_GROUP` | Sí | — | Consumer group del worker. |
| `KAFKA_CLIENT_ID` | No | `boxiaracing-telemetry-worker` | Identificador del cliente. |
| `KAFKA_TLS_ENABLED` | No | `true` | Activa TLS 1.2 o superior. |
| `KAFKA_SASL_MECHANISM` | No | `none` | `none`, `plain`, `scram-sha-256` o `scram-sha-512`. |
| `KAFKA_SASL_USERNAME` | Según SASL | — | Usuario SASL. |
| `KAFKA_SASL_PASSWORD` | Según SASL | — | Contraseña SASL. |
| `KAFKA_OFFSET_RESET` | No | `earliest` | `earliest` o `latest`. |
| `KAFKA_MAX_POLL_RECORDS` | No | `100` | Máximo de mensajes obtenidos por poll. |
| `LOG_LEVEL` | No | `info` | `debug`, `info`, `warn` o `error`. |

`.env.example` contiene una configuración local sin credenciales.

## Desarrollo

Requiere Go 1.25 o superior:

```bash
go test ./...
go vet ./...
go run ./cmd/worker
```

También puede verificarse sin instalar Go:

```bash
docker build -t boxiaracing-telemetry-worker:local .
docker run --rm --env-file .env boxiaracing-telemetry-worker:local
```

## Formato de la DLQ

Cada mensaje contiene `source_topic`, `source_partition`, `source_offset`,
`source_timestamp`, `key_base64`, `payload_base64`, `error_type`,
`error_message` y `failed_at`. La key y el payload originales siempre se
codifican en Base64 para que cualquier secuencia de bytes pueda representarse.

## Incorporar el transformador real

1. Modelar las estructuras descritas por el documento de telemetría dentro de
   `internal/transform`.
2. Implementar `Transformer.Transform(context.Context, []byte)`.
3. Añadir fixtures y tests para cada estructura y cada campo Base64 conocido.
4. Reemplazar `transform.JSONPassthrough{}` en `cmd/worker/main.go`.

La detección heurística de Base64 queda explícitamente fuera del diseño.

## Render

`render.yaml` declara un background worker Docker de plan `starter`. Al crear
el Blueprint, Render solicitará brokers, tópicos y credenciales marcados con
`sync: false`. Los secretos no deben agregarse al repositorio.

Render enviará `SIGTERM` durante un redeploy y esperará hasta 60 segundos. El
worker deja de consumir, no confirma el mensaje que esté incompleto y cierra el
cliente Kafka antes de salir.
