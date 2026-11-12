# Guía 3.3.2 — Tuning (buffers, retention) y pruebas de carga

Escenario IoT: miles de sensores envían datos a la vez. Se configura un productor de alto
rendimiento (batching + compresión), un broker con retención agresiva y un consumidor lento
para provocar **Consumer Lag**.

```
GET /api/test/start?count=N
        │  KafkaTemplate tuneado (batch.size 32KB, linger.ms 20, snappy)
        ▼
sensor-topic (3 particiones, retención 5 min / 50 MB)
        │
        ▼
SlowConsumer (analytics-group, 10 ms por mensaje = máx 100 msg/s)  ──►  LAG
```

## Cómo correrlo

1. Levantar Kafka (desde esta carpeta): `docker compose up -d`
2. Levantar la app: `.\mvnw.cmd spring-boot:run` (Windows) o `./mvnw spring-boot:run`
3. App en **http://localhost:8888**, Kafka UI en **http://localhost:8081**.

> No se puede correr al mismo tiempo que las guías 3.2.1 / 3.2.2 (usan los mismos puertos).
> Antes, apagar la otra con `docker compose down` en su carpeta.

El topic `sensor-topic` (3 particiones) lo crea la app al arrancar (`TopicConfig`), así que el
Paso 2 de la guía es opcional. Si lo querés hacer a mano igual:

```bash
docker exec -it kafka-tuning-kafka-1 kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 --topic sensor-topic --partitions 3 --replication-factor 1
```

## Pruebas

### Prueba de carga y Consumer Lag

```powershell
Invoke-RestMethod "http://localhost:8888/api/test/start?count=100000"
```

En Kafka UI → **Consumers → analytics-group** se ve el Lag alto bajando de a ~100 por segundo.
Por terminal:

```bash
docker exec kafka-tuning-kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group analytics-group
```

### Retención

```powershell
Invoke-RestMethod "http://localhost:8888/api/test/start?count=500000"
```

Esperar 5–7 minutos y mirar Kafka UI → **Topics → sensor-topic**: el tamaño baja y los
mensajes viejos desaparecen aunque el consumidor no los haya leído.

## Diferencias con el código de la guía

- **Puertos:** app en 8888 y Kafka UI en 8081 (en esta PC el 8080 lo ocupa Oracle XE).
- **`KAFKA_LOG_SEGMENT_BYTES: 1048576`** en `docker-compose.yml`: Kafka solo borra segmentos
  *cerrados*, y el segmento por defecto es de 1 GB. Gracias a snappy + batching, 600.000
  mensajes ocupan apenas ~7 MB en disco, así que con la configuración de la guía ningún
  segmento se cierra y la retención nunca se ve. Por lo mismo, el límite de 50 MB
  (`retention.bytes`) no se alcanza: lo que se observa es la retención **por tiempo** (5 min).
- **`TopicConfig`** crea `sensor-topic` con 3 particiones al arrancar.
- **División por cero** evitada en el cálculo de throughput cuando la carga es muy chica.
