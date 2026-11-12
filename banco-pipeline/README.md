# Guía 3.2.2 — Pipeline de eventos end-to-end (Kafka Streams)

Sistema de monitoreo de transacciones bancarias en tiempo real.

```
POST /api/banco/transaccion
        │
        ▼
banco-transacciones ──► Kafka Streams (PipelineTopology)
                          ├─ Filtro: ignora monto <= 10
                          ├─ Enriquecimiento: calcula montoUsd (1 USD = 950, simulado)
                          ├─ Detección: monto > 10.000 ──► banco-alertas
                          └─ Agregación: total por usuario ──► banco-totales-usuario
```

## Cómo correrlo

1. Levantar Kafka (desde esta carpeta): `docker-compose up -d`
2. Levantar la app: `.\mvnw.cmd spring-boot:run` (Windows) o `./mvnw spring-boot:run`
3. La app corre en **http://localhost:8888** y Kafka UI en **http://localhost:8081**
   (en esta PC el 8080 lo ocupa Oracle XE; si en la tuya está libre, podés volver a `"8080:8080"` en `docker-compose.yml`).

## Pruebas (PowerShell)

```powershell
# Normal: se procesa, NO genera alerta
Invoke-RestMethod -Method Post -Uri http://localhost:8888/api/banco/transaccion -ContentType "application/json" -Body '{"usuario": "juan", "monto": 500, "origen": "app"}'

# Fraudulenta: genera alerta
Invoke-RestMethod -Method Post -Uri http://localhost:8888/api/banco/transaccion -ContentType "application/json" -Body '{"usuario": "juan", "monto": 15000, "origen": "cajero"}'

# Ignorada por el filtro (5 < 10)
Invoke-RestMethod -Method Post -Uri http://localhost:8888/api/banco/transaccion -ContentType "application/json" -Body '{"usuario": "juan", "monto": 5, "origen": "app"}'
```

Con curl (Git Bash / Linux):

```bash
curl -X POST http://localhost:8888/api/banco/transaccion \
   -H "Content-Type: application/json" \
   -d '{"usuario": "juan", "monto": 15000, "origen": "cajero"}'
```

## Verificación visual

En Kafka UI (http://localhost:8081) → Topics → `banco-alertas` → Messages:

```json
{ "usuario": "juan", "monto": 15000.0, "motivo": "Transacción inusualmente alta en cajero", "severidad": "ALTA" }
```

## Diferencias con el código de la guía

- Spring Boot 4 / Spring Kafka 4: se usa `JacksonJsonSerde` / `JacksonJsonSerializer` en vez de `JsonSerde` (deprecado).
- Se agregó `@EnableKafkaStreams` en la clase principal (sin eso el pipeline no arranca).
- Puerto 8888 en vez de 8080 (choca con Kafka UI).
- `TopicsConfig` crea los topics al arrancar: Kafka Streams no crea el topic de entrada y, si
  `banco-transacciones` no existe, el pipeline se detiene con `MissingSourceTopicException`.

## Resultado de la prueba

```
✅ Transacción procesada: 500.0
✅ Transacción procesada: 15000.0
🚨 ALERTA GENERADA: Transacción inusualmente alta en cajero
💰 Total acumulado de juan: 15500.0
```

La transacción de 5 no aparece (filtrada). El total por usuario se publica cada ~30 s
(intervalo de commit de Kafka Streams), por eso aparece una sola vez con la suma de ambas.
- Se implementaron el enriquecimiento (USD) y la agregación por usuario que pide el contexto de la guía y que el código de ejemplo no incluía.
