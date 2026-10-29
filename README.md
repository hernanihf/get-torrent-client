# ⚡ get-torrent

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://golang.org)
[![BitTorrent](https://img.shields.io/badge/BitTorrent-BEP%200003%20%7C%200015%20%7C%200023-blue)](https://www.bittorrent.org/beps/bep_0000.html)
[![Transmission RPC](https://img.shields.io/badge/Transmission%20RPC-v17%20Compatible-red)](https://github.com/transmission/transmission/blob/main/docs/rpc-spec.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`get-torrent` es un cliente y daemon BitTorrent de alto rendimiento, modular y ligero escrito en **Go**, diseñado como una reimplementación moderna compatible con el ecosistema de **Transmission**.

Expone una interfaz JSON-RPC idéntica a la de Transmission en el puerto `9091`, permitiendo ser controlado de forma nativa por aplicaciones existentes como **Transmission Remote GUI**, la CLI `transmission-remote`, clientes móviles y WebUIs.

---

## 🚀 Características Principales

### 🌐 BitTorrent Core Engine
- **Decodificador y Codificador Bencode nativo**: Implementación completa y optimizada para metainfo (`.torrent`) y respuestas de trackers.
- **Soporte Dual de Trackers**:
  - **HTTP/HTTPS Trackers**: Anuncios con soporte para respuestas compactas ([BEP 0023](https://www.bittorrent.org/beps/bep_0023.html)).
  - **UDP Trackers ([BEP 0015](https://www.bittorrent.org/beps/bep_0015.html))**: Protocolo binario de baja latencia con transacción en 2 fases (`connect` y `announce`).
- **Peer Wire Protocol ([BEP 0003](https://www.bittorrent.org/beps/bep_0003.html))**: Handshake estricto de 68 bytes, framing de mensajes, intercambio de bitfields y pipelining de bloques de 16 KiB.
- **Magnet Links ([BEP 0009](https://www.bittorrent.org/beps/bep_0009.html) / [BEP 0010](https://www.bittorrent.org/beps/bep_0010.html))**: Descarga del diccionario `info` desde peers vía `ut_metadata`, verificación SHA-1 contra el `info_hash`, persistencia como `.torrent` en `config/torrents/` y servicio de metadatos a otros peers.
- **Multi-tracker (`announce-list`)**: Anuncio concurrente a todos los trackers con fusión y deduplicación de peers.
- **Mainline DHT ([BEP 0005](https://www.bittorrent.org/beps/bep_0005.html))**: Nodo Kademlia sobre UDP (mismo puerto que los peers) con k-buckets, búsquedas iterativas con *soft timeouts*, tokens rotativos para `announce_peer`, protección anti-spoofing y persistencia del ID y la tabla de ruteo en `config/dht.json`. Permite descargar magnets sin trackers. Los torrents privados ([BEP 0027](https://www.bittorrent.org/beps/bep_0027.html)) nunca usan la DHT.
- **Peer Exchange ([BEP 0011](https://www.bittorrent.org/beps/bep_0011.html))**: Mensajes `ut_pex` cada minuto con los peers conectados y desconectados (hasta 50 por lista, IPv4 e IPv6, con *flags* de seed/alcanzable); los peers recibidos se conectan y se informan en `peersFrom.fromPex`. El puerto de escucha se anuncia en el *handshake* de extensiones (`p`) para poder compartir peers entrantes. Nunca se usa en torrents privados; se controla con `pex-enabled`.
- **Eventos y calendario de anuncios por tracker (BEP 0003 / 0015)**: El primer anuncio de cada sesión lleva `started`, al terminar la descarga se envía `completed` y al detener (o apagar el daemon) se envía `stopped` a los trackers que aceptaron `started`. Cada tracker se re-anuncia según el `interval` que devuelve (respetando `min interval`, también en `torrent-reannounce`), con *back-off* exponencial ante fallos; `tracker id`, `key` y `numwant` se envían como indica el spec. Los peers de cada tracker se conectan apenas responde, sin esperar al resto. La DHT se consulta cada 5 minutos (cada minuto si no hay peers). Los fallos se reportan en `errorString` sin detener el torrent, y `trackerStats` informa los seeders/leechers del último anuncio.
- **UDP trackers robustos (BEP 0015)**: Retransmisión con espera `15·2ⁿ` s (hasta 4 intentos), reutilización del *connection ID* durante un minuto y descarte de respuestas con otro *transaction ID*.

### 🧠 Swarm & Algoritmos de Descarga
- **Planificación Rarest-First**: Mapea en tiempo real la disponibilidad de piezas en el enjambre y prioriza las piezas más escasas para optimizar la salud del enjambre P2P.
- **Modo Endgame**: Al acercarse al 100%, solicita los últimos bloques a todos los peers disponibles en paralelo y envía ráfagas de `MsgCancel` en cuanto llega el primero para evitar cuellos de botella.
- **Subida & Seeding**: Responde a peticiones entrantes `request`, lee fragmentos desde disco y sirve bloques a peers interesados con desestrangulamiento (*unchoking*).
- **Algoritmo de *choking* (BEP 0003)**: Cada 10 s se desestrangula a los 4 peers interesados con mejor tasa (*tit-for-tat*: la que nos envían al descargar, la que les enviamos al sembrar) más 1 *optimistic unchoke* que rota cada 30 s, con el triple de probabilidad para peers recién conectados. Los cambios de interés y las desconexiones re-evalúan los slots de inmediato; las peticiones de peers estrangulados se ignoran.
- **Cola de subida por peer**: Las peticiones `request` se encolan (hasta 250, anunciado como `reqq`) y las sirve una goroutine aparte, de modo que un `cancel` del peer (típico en su *endgame*) retira el bloque aunque esté esperando ancho de banda, y estrangular al peer descarta su cola (BEP 0003).

### 💾 Almacenamiento e Integridad
- **Soporte Single-File y Multi-File**: Reconstrucción de jerarquías de carpetas complejas.
- **Mapeo de límites de archivos (*Boundary Spanning*)**: Escritura atómica mediante `io.WriterAt` (`os.File.WriteAt`) para piezas que cruzan físicamente entre múltiples archivos.
- **Verificación Criptográfica SHA-1**: Validación pieza por pieza antes de persistir a disco y anunciar `have` al enjambre.

### 🔌 Compatibilidad Transmission RPC
Implementa el protocolo RPC de **Transmission 4.0.x** (`rpc-version` 17, dialecto clásico con nombres camelCase/kebab-case), verificado contra `transmission-remote` 4.1.4 real.
- **Seguridad**: token CSRF `X-Transmission-Session-Id` (HTTP 409), autenticación HTTP Basic opcional y lista blanca del header `Host` contra ataques de *DNS rebinding* (HTTP 421), en el mismo orden que Transmission.
- **`torrent-get`**: ~70 campos con los nombres exactos del spec (`status`/`error` numéricos, `rateDownload`, `eta`, `files`, `fileStats`, `peers`, `trackerStats`, `pieces`, `availability`, ...), filtro `fields`, formato `table` e `ids` por número, hash o `recently-active` (con `removed`).
- **`torrent-add`**: ruta local, URL `http(s)`, enlace magnet o `.torrent` en base64 (`metainfo`), `download-dir` por torrent, `paused`, y respuesta `torrent-duplicate` para torrents repetidos.
- **Ciclo de vida**: `torrent-start`, `torrent-start-now`, `torrent-stop`, `torrent-remove` (con `delete-local-data`), `torrent-verify` (re-chequeo SHA-1 de los datos en disco) y `torrent-reannounce`.
- **Sesión**: `session-get` (todas las claves, con filtro `fields`), `session-set`, `session-stats` (estadísticas actuales y acumuladas entre reinicios), `free-space`, `port-test` y `session-close` (apagado ordenado).
- **Límites de velocidad**: límites de sesión (`speed-limit-down/up`), velocidades alternativas o *turtle mode* (`alt-speed-*`) y límites por torrent (`downloadLimit`/`uploadLimit` y `honorsSessionLimits` vía `torrent-set`), en kB/s de 1000 bytes como Transmission. Se aplican con *token buckets*: la subida espera antes de enviar cada bloque y la bajada pausa la lectura del socket para que el control de flujo de TCP frene al peer. Un límite activo de 0 pausa la transferencia.
- **Ajustes honestos**: `session-set` aplica `download-dir` y los límites de velocidad; si se pide cambiar un ajuste que get-torrent no implementa (p. ej. `pex-enabled`, `encryption`, `alt-speed-time-*`) responde con error nombrando las claves y no aplica nada. Repetir el valor que informa `session-get` se acepta, así las GUIs que envían todo el diálogo de preferencias siguen funcionando; las claves desconocidas o de solo lectura se ignoran como en Transmission.
- **Selección de archivos y prioridades**: `files-wanted`/`files-unwanted` y `priority-high/normal/low` en `torrent-add` y `torrent-set` (lista vacía = todos los archivos). Las piezas de archivos no deseados no se piden; las de mayor prioridad van primero y, dentro de una prioridad, las más raras. Al completar lo seleccionado el torrent pasa a sembrar (*partial seed*) y vuelve a descargar si se seleccionan más archivos. `percentDone`, `sizeWhenDone`, `leftUntilDone`, `wanted`, `priorities` y `fileStats` reflejan la selección, que se guarda en `state.json`. Los archivos no deseados igual se pre-asignan en disco.
- **Límites de siembra**: `seedRatioLimit`/`seedRatioLimited` e `idle-seeding-limit`/`idle-seeding-limit-enabled` de sesión, y por torrent `seedRatioMode`/`seedIdleMode` (0 = sesión, 1 = propio, 2 = ilimitado) con sus límites vía `torrent-set`. Al alcanzar el ratio (subido / descargado, o / tamaño si se agregó completo) o el tiempo sin subir datos, el torrent se detiene e informa `isFinished`; iniciarlo a mano desactiva su ratio, como Transmission. `eta` al sembrar indica el tiempo hasta el ratio y `etaIdle` el restante hasta el límite de inactividad.
- **No soportado aún**: `torrent-set` ignora los demás ajustes que no son de ancho de banda ni de archivos; `torrent-set-location`, `torrent-rename-path`, `blocklist-update` y `group-*` devuelven error; el horario automático del *turtle mode* (`alt-speed-time-*`) no está implementado.
- **Persistencia de Sesión (Resume Data)**: Guardado y recuperación atómica de descargas previas en `config/state.json`. Cada torrent guarda su *bitfield* de piezas verificadas junto con el tamaño y la fecha de modificación de sus archivos; al arrancar se restaura sin re-hashear si los archivos no cambiaron. Si cambiaron (o no hay *bitfield*, o se cortó un chequeo), los datos en disco se re-chequean (SHA-1 por pieza) antes de reanudar, de a un torrent por vez como la cola de verificación de Transmission.

---

## 🏗️ Arquitectura del Sistema

```
                      ┌──────────────────────────────────────────────┐
                      │ Clientes Remotos (Transmission GUI, WebUI)   │
                      └──────────────────────┬───────────────────────┘
                                             │ HTTP JSON-RPC (:9091)
                                             ▼
┌────────────────────────────────────────────────────────────────────┐
│ get-torrent Daemon                                                 │
│                                                                    │
│  ┌────────────────────────┐         ┌───────────────────────────┐  │
│  │ Transmission RPC Server│◄───────►│ BitTorrent Engine         │  │
│  └────────────────────────┘         └─────────────┬─────────────┘  │
│                                                   │                │
│            ┌──────────────────────┬───────────────┴────────────┐   │
│            ▼                      ▼                            ▼   │
│   ┌─────────────────┐    ┌─────────────────┐          ┌─────────┐  │
│   │ Tracker Client  │    │ Peer Wire Conn  │          │ Storage │  │
│   │ (HTTP & UDP)    │    │ (Rarest-First)  │          │ Engine  │  │
│   └────────┬────────┘    └────────┬────────┘          └────┬────┘  │
└────────────┼──────────────────────┼────────────────────────┼───────┘
             │                      │                        │
             ▼                      ▼                        ▼
      Trackers P2P              Swarm Peers            Disco Local
   (ubuntu.com, etc)        (TCP Data Blocks)         (./downloads)
```

---

## 🛠️ Instalación y Compilación

### Prerrequisitos
- **Go 1.23+** instalado (`brew install go` en macOS o `sudo apt install golang` en Linux).

### Compilación desde código fuente
```bash
# Clonar el repositorio
git clone git@github.com:hernanihf/get-torrent-client.git
cd get-torrent-client

# Compilar el binario del daemon
go build -o bin/get-torrent-daemon ./cmd/daemon
```

---

## 🚦 Uso Rápido

### 1. Iniciar el Daemon
```bash
./bin/get-torrent-daemon --rpc-addr 127.0.0.1:9091 --download-dir ./downloads
```

Parámetros disponibles:
- `--rpc-addr`: Dirección y puerto de escucha del servidor RPC (por defecto `127.0.0.1:9091`).
- `--download-dir`: Directorio donde se guardarán los archivos completados (por defecto `./downloads`).
- `--config-dir`: Carpeta donde se almacena el archivo `state.json` de sesión (por defecto `./config`).
- `--save-interval`: Cada cuánto se guarda `state.json` mientras el daemon corre, para no perder la sesión ante un corte (por defecto `5m`; `0` guarda solo al apagar).
- `--port`: Puerto de escucha para conexiones entrantes de peers (TCP) y para la DHT (UDP) (por defecto `6881`).
- `--dht`: Habilita la DHT (por defecto `true`).
- `--dht-bootstrap`: Routers de arranque de la DHT separados por comas (por defecto `router.bittorrent.com:6881`, `dht.transmissionbt.com:6881`, ...).
- `--rpc-username`: Exige autenticación HTTP Basic en el RPC. La contraseña se toma de la variable de entorno `GET_TORRENT_RPC_PASSWORD` (recomendado, no aparece en `ps`) o de `--rpc-password`.
- `--rpc-host-whitelist`: Nombres de host adicionales permitidos en el header `Host` (las IPs y `localhost` siempre se aceptan; `*.dominio` admite subdominios).

### 2. Controlar mediante Transmission RPC (`curl`)
Obtener el token de sesión CSRF y listar torrents:
```bash
# 1. Obtener Session ID
SESSION_ID=$(curl -s -D - http://127.0.0.1:9091/transmission/rpc | grep X-Transmission-Session-Id | cut -d' ' -f2 | tr -d '\r')

# 2. Consultar información de la sesión
curl -s -X POST http://127.0.0.1:9091/transmission/rpc \
  -H "X-Transmission-Session-Id: $SESSION_ID" \
  -d '{"method":"session-get"}' | jq

# 3. Añadir una descarga
curl -s -X POST http://127.0.0.1:9091/transmission/rpc \
  -H "X-Transmission-Session-Id: $SESSION_ID" \
  -d '{"method":"torrent-add","arguments":{"filename":"/ruta/a/archivo.torrent"}}' | jq
```

### 3. Usar `transmission-remote`
```bash
transmission-remote 127.0.0.1:9091 -a "magnet:?xt=urn:btih:..."   # añadir
transmission-remote 127.0.0.1:9091 -l                            # listar
transmission-remote 127.0.0.1:9091 -t 1 -i                       # detalle
transmission-remote 127.0.0.1:9091 -t 1 --verify                 # re-chequear datos en disco
transmission-remote 127.0.0.1:9091 -d 2000                       # límite de bajada de sesión (kB/s)
transmission-remote 127.0.0.1:9091 -asd 500 -as                  # turtle mode a 500 kB/s
transmission-remote 127.0.0.1:9091 -t 1 -u 100                   # límite de subida por torrent
transmission-remote 127.0.0.1:9091 --exit                        # apagar el daemon
```

También podés conectar directamente cualquier cliente como **Transmission Remote GUI** configurando Host: `127.0.0.1` y Puerto: `9091`.

---

## 🧪 Pruebas Unitarias

El proyecto cuenta con una cobertura integral de pruebas unitarias y de integración en memoria para todos los subsistemas:

```bash
go test -v ./...
```

Paquetes testeados:
- `pkg/bencode`: Codificación y decodificación de enteros, cadenas con prefijo, listas y diccionarios ordenados.
- `pkg/tracker`: Parseo de peers compactos BEP 0023 y consultas HTTP simuladas en memoria.
- `pkg/peer`: Handshake BitTorrent de 68 bytes, framing de mensajes y simulación de conexión de sockets con `net.Pipe`.
- `pkg/storage`: Verificación SHA-1, ensamblado de bloques de 16 KB, escritura multifile y algoritmos Rarest-First y Endgame.
- `pkg/bandwidth`: *token bucket* (tasa, límite 0, cambios en caliente, cancelación); en `pkg/engine`, transferencias reales cronometradas con cada tipo de límite.
- `pkg/rpc`: CSRF, autenticación, lista blanca de `Host`, filtro de campos y formato `table`, selección de `ids`, `torrent-duplicate`, `torrent-verify` con datos corruptos, `session-stats`, `free-space` y `session-close`.
- `pkg/engine`: Persistencia atómica de sesión (`SaveState` / `LoadState`) y descarga end-to-end desde un magnet entre dos engines reales.
- `pkg/peer`, `pkg/metadata`: Mensajes BEP 0010/0009 y ensamblado/verificación de metadatos.
- `pkg/dht`: KRPC, tabla de ruteo, tokens, paquetes falsificados/malformados, red de 30 nodos en localhost y ausencia de fugas de goroutines; en `pkg/engine`, descarga end-to-end de un magnet sin trackers vía DHT.
- `pkg/metainfo`: Info-hash sobre bytes crudos, `announce-list` y rechazo de rutas inseguras (`..`, absolutas).

---

## 🤖 Desarrollo Multiagente

Este proyecto fue desarrollado bajo una arquitectura multiagente coordinada:
- **Controlador (Primary Agent)**: Orquestador general del ciclo de vida y cumplimiento de especificaciones.
- **Analista de Protocolo (`torrent-analyst`)**: Especificación técnica de BEPs de BitTorrent y estándares RPC.
- **Desarrollador (`torrent-coder`)**: Implementación en Go de alta concurrencia, networking y almacenamiento.
- **QA & Testing (`torrent-qa`)**: Diseño de suites de pruebas unitarias y validación con mocks.

Consulta [AGENTS.md](AGENTS.md) para más detalles sobre las reglas del equipo y el Definition of Done (DoD).

---

## 🗺️ Roadmap

- [x] **Fase 1: Descarga y Almacenamiento**: Piece manager (16 KB), verificación SHA-1 y escritura multi-archivo en disco.
- [x] **Fase 2: Estrategia P2P**: Algoritmo Rarest-First, modo Endgame con cancelaciones y servicio de piezas (seeding).
- [x] **Fase 3: Transmission RPC Completo**: Endpoints de control de ciclo de vida (`start`, `stop`, `remove`), configuración en caliente y persistencia de sesión (`state.json`).
- [x] **Fase 4: Magnet Links & DHT**
  - [x] **4a – Magnet Links**: Parser de enlaces magnet (`magnet:?xt=...`), Extension Protocol ([BEP 0010](https://www.bittorrent.org/beps/bep_0010.html)) y descarga/servicio de metadatos `ut_metadata` ([BEP 0009](https://www.bittorrent.org/beps/bep_0009.html)).
  - [x] **4b – Kademlia DHT** ([BEP 0005](https://www.bittorrent.org/beps/bep_0005.html)): descubrimiento de peers sin trackers, `announce_peer` y mensaje `PORT`.
- [ ] **Fase 5: Clientes & WebUI**: CLI nativo (`get-torrent-cli`) y Web Dashboard interactivo integrado en el daemon.

---

## 📄 Licencia

Distribuido bajo la Licencia MIT. Consulta `LICENSE` para más información.
