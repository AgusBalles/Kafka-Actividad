# Actividad Kafka — Particiones con Spring Boot

Mini aplicación de ejemplo para la actividad de **Apache Kafka**: un productor (endpoint REST) y un consumidor, más dos scripts para enviar muchas peticiones de prueba (uno para Linux/macOS/Git Bash/WSL y otro para Windows PowerShell).

---

## Índice

- [Arquitectura](#arquitectura)
- [Requisitos](#requisitos)
- [Cómo levantar el entorno](#cómo-levantar-el-entorno)
- [Endpoints](#endpoints)
- [Scripts de carga](#scripts-de-carga)
  - [Linux / macOS / Git Bash / WSL (`send-many.sh`)](#linux--macos--git-bash--wsl-send-manysh)
  - [Windows PowerShell (`send-many.ps1`)](#windows-powershell-send-manyps1)
- [Cómo ver los resultados](#cómo-ver-los-resultados)
- [Problemas comunes](#problemas-comunes)
- [Estructura del proyecto](#estructura-del-proyecto)

---

## Arquitectura

```
Cliente (curl / script)
        │  POST o GET /api/kafka/send?message=...&key=...
        ▼
KafkaController (Spring Web)
        │  KafkaTemplate.send(topic, key, message)
        ▼
Kafka broker (docker)  ──►  topic: tutorial-particiones (3 particiones)
        │
        ▼
KafkaConsumer (@KafkaListener, group: mi-grupo-educativo)
        │  imprime partición, offset, key y mensaje en consola
        ▼
Log de la aplicación
```

- **Topic:** `tutorial-particiones`
- **Particiones:** 3
- **Grupo de consumo:** `mi-grupo-educativo`
- **Puerto de la app:** `8888`
- **Kafka UI:** `http://localhost:8080`

---

## Requisitos

- **Java 21** (`java -version`)
- **Docker** + **Docker Compose**
- El proyecto incluye el **Maven Wrapper**, no necesitas instalar Maven.

---

## Cómo levantar el entorno

1. Levantar Kafka + ZooKeeper + Kafka UI:

   ```bash
   docker compose up -d
   ```

2. Arrancar la aplicación:

   **Linux / macOS / Git Bash / WSL**

   ```bash
   ./mvnw spring-boot:run
   ```

   **Windows (CMD / PowerShell)**

   ```powershell
   .\mvnw.cmd spring-boot:run
   ```

3. Verificar que responde (debe dar `HTTP 400` porque falta el parámetro `message`, pero confirma que la app está arriba):

   ```bash
   curl -i http://localhost:8888/api/kafka/send
   ```

---

## Endpoints

| Método | Ruta | Parámetros | Descripción |
| ------ | ---- | ---------- | ----------- |
| `POST` / `GET` | `/api/kafka/send` | `message` (obligatorio), `key` (opcional) | Publica un mensaje en el topic. |

Ejemplos:

```bash
# POST
curl -X POST "http://localhost:8888/api/kafka/send?message=hola&key=orden-1"

# GET (también funciona desde el navegador)
curl "http://localhost:8888/api/kafka/send?message=Paso1&key=orden-555"
```

Respuesta:

```
Mensaje enviado: hola | Key: orden-1
```

> La `key` define la partición destino mediante el hash `murmur2` de Kafka. La misma key siempre va a la misma partición.

---

## Scripts de carga

Ambos scripts envían muchas peticiones al endpoint, generan keys rotativas (`orden-0`, `orden-1`, ...) para repartir la carga entre las 3 particiones y muestran un resumen (total, OK, fallidos, tiempo y mensajes/segundo).

### Linux / macOS / Git Bash / WSL (`send-many.sh`)

Dar permisos de ejecución la primera vez:

```bash
chmod +x send-many.sh
```

Uso:

```bash
./send-many.sh              # 100 mensajes, secuencial
./send-many.sh 500          # 500 mensajes
./send-many.sh 1000 20      # 1000 mensajes con 20 peticiones en paralelo
```

Variables de entorno opcionales:

| Variable | Default | Descripción |
| -------- | ------- | ----------- |
| `URL` | `http://localhost:8888/api/kafka/send` | Endpoint destino |
| `COUNT` | `100` | Cantidad de mensajes |
| `PARALLEL` | `1` | Peticiones simultáneas |
| `KEY_POOL` | `12` | Cantidad de keys distintas |
| `KEY_PREFIX` | `orden` | Prefijo de la key |
| `MSG_PREFIX` | `mensaje` | Prefijo del mensaje |
| `METHOD` | `POST` | `GET` o `POST` |
| `TIMEOUT` | `15` | Timeout por petición (segundos) |
| `VERBOSE` | `0` | `1` muestra cada petición |

Ejemplos:

```bash
PARALLEL=10 ./send-many.sh 2000
KEY_POOL=3 PARALLEL=5 ./send-many.sh 300
METHOD=GET ./send-many.sh 50
```

### Windows PowerShell (`send-many.ps1`)

Funciona en **Windows PowerShell 5.1** y en **PowerShell 7+**. La opción `-Parallel` (más de 1) requiere **PowerShell 7+**; en 5.1 el script avisa y ejecuta secuencialmente.

Uso:

```powershell
.\send-many.ps1                                  # 100 mensajes, secuencial
.\send-many.ps1 -Count 500                       # 500 mensajes
.\send-many.ps1 -Count 1000 -Parallel 20         # 1000 mensajes, 20 en paralelo
```

Si PowerShell bloquea el script por la política de ejecución:

```powershell
powershell -ExecutionPolicy Bypass -File .\send-many.ps1 -Count 500
```

Parámetros:

| Parámetro | Default | Descripción |
| --------- | ------- | ----------- |
| `-Count` | `100` | Cantidad de mensajes |
| `-Parallel` | `1` | Peticiones simultáneas (requiere PS 7+) |
| `-Url` | `http://localhost:8888/api/kafka/send` | Endpoint destino |
| `-KeyPool` | `12` | Cantidad de keys distintas |
| `-KeyPrefix` | `orden` | Prefijo de la key |
| `-MsgPrefix` | `mensaje` | Prefijo del mensaje |
| `-Method` | `POST` | `GET` o `POST` |
| `-Timeout` | `15` | Timeout por petición (segundos) |
| `-VerboseOut` | — | Muestra cada petición |

Ejemplos:

```powershell
.\send-many.ps1 -Count 2000 -Parallel 10
.\send-many.ps1 -Count 300 -KeyPool 3
.\send-many.ps1 -Count 50 -Method GET
```

> **Alternativa en Windows:** si tienes Git Bash o WSL, también puedes ejecutar `send-many.sh` tal cual.

---

## Cómo ver los resultados

**1. Consola de la aplicación:** cada mensaje consumido imprime un bloque como este:

```
====================================================================================================
MENSAJE RECIBIDO
Particion: 1
Offset: 4
Key: orden-3
Mensaje: mensaje-15
====================================================================================================
```

**2. Kafka UI:** <http://localhost:8080> → *Topics* → `tutorial-particiones` → *Messages*.

**3. Mensajes por partición (offsets):**

```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic tutorial-particiones
```

**4. Estado del grupo de consumo (LAG):**

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group mi-grupo-educativo
```

Con el consumidor activo, `LAG` debe tender a `0`.

---

## Problemas comunes

### El navegador muestra `405 Method Not Allowed`

El navegador siempre hace `GET`. El endpoint acepta `GET` y `POST`, pero si ves un 405 verifica que el controlador esté actualizado y reinicia la app.

### El `curl` se queda colgado ~60 segundos y no responde

El broker de Kafka está caído. El productor espera metadata del broker hasta agotar `max.block.ms`. Revisa:

```bash
docker ps -a | grep kafka
docker logs --tail 50 kafka
```

Si aparece `KeeperException$NodeExistsException` al arrancar, el estado de ZooKeeper quedó corrupto. Reinicia todo limpio (no basta con `docker start kafka`):

```bash
docker compose down
docker compose up -d
```

### Reiniciar el topic a cero

Para borrar los mensajes y offsets y dejar el topic con 3 particiones vacías:

```bash
# 1. Detener la app (Ctrl+C)
# 2. Borrar y recrear el topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic tutorial-particiones
sleep 10
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic tutorial-particiones --partitions 3 --replication-factor 1

# 3. Borrar los grupos de consumo
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --delete --group mi-grupo-educativo
```

> Detén la app antes de borrar el topic: si está suscrita, el cliente puede recrear el topic automáticamente con 1 sola partición.

### Las keys caen todas en la misma partición

No es un error: el hash `murmur2` de Kafka puede asignar varias keys seguidas a la misma partición. Usa `KEY_POOL=12` (valor por defecto) o más para ver la distribución.

---

## Estructura del proyecto

```
.
├── docker-compose.yml          # ZooKeeper + Kafka + Kafka UI
├── pom.xml                     # Spring Boot 4 + Spring for Apache Kafka
├── mvnw / mvnw.cmd             # Maven Wrapper
├── send-many.sh                # Script de carga (Linux/macOS/Git Bash/WSL)
├── send-many.ps1               # Script de carga (Windows PowerShell)
└── src
    ├── main
    │   ├── java/com/kafka
    │   │   ├── KafkaApplication.java
    │   │   ├── consumer/KafkaConsumer.java
    │   │   └── controller/KafkaController.java
    │   └── resources/application.yml
    └── test/java/com/kafka/KafkaApplicationTests.java
```
