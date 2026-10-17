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

### 🧠 Swarm & Algoritmos de Descarga
- **Planificación Rarest-First**: Mapea en tiempo real la disponibilidad de piezas en el enjambre y prioriza las piezas más escasas para optimizar la salud del enjambre P2P.
- **Modo Endgame**: Al acercarse al 100%, solicita los últimos bloques a todos los peers disponibles en paralelo y envía ráfagas de `MsgCancel` en cuanto llega el primero para evitar cuellos de botella.
- **Subida & Seeding**: Responde a peticiones entrantes `request`, lee fragmentos desde disco y sirve bloques a peers interesados con desestrangulamiento (*unchoking*).

### 💾 Almacenamiento e Integridad
- **Soporte Single-File y Multi-File**: Reconstrucción de jerarquías de carpetas complejas.
- **Mapeo de límites de archivos (*Boundary Spanning*)**: Escritura atómica mediante `io.WriterAt` (`os.File.WriteAt`) para piezas que cruzan físicamente entre múltiples archivos.
- **Verificación Criptográfica SHA-1**: Validación pieza por pieza antes de persistir a disco y anunciar `have` al enjambre.

### 🔌 Compatibilidad Transmission RPC
- **Protección CSRF**: Autenticación mediante token de sesión `X-Transmission-Session-Id` (código HTTP 409 inicial).
- **Control Total de Torrents**:
  - `torrent-get`: Estadísticas en tiempo real (progreso, velocidades, peers conectados, tamaños).
  - `torrent-add`: Carga de torrents por ruta local.
  - `torrent-start` / `torrent-start-now`: Reanudación de descargas concurrentes.
  - `torrent-stop`: Pausa controlada con desconexión ordenada de sockets.
  - `torrent-remove`: Eliminación de torrents con opción `delete-local-data`.
  - `session-get` / `session-set`: Configuración de directorios y límites de ancho de banda.
  - `port-test`: Diagnóstico de conectividad de puerto.
- **Persistencia de Sesión (Resume Data)**: Guardado y recuperación atómica de descargas previas en `config/state.json`.

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
- `--port`: Puerto de escucha para conexiones entrantes de peers (por defecto `6881`).

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
- `pkg/rpc`: Protección contra CSRF 409 y endpoints de control de torrents.
- `pkg/engine`: Persistencia atómica de sesión (`SaveState` / `LoadState`).

---

## 🤖 Desarrollo Multiagente (Google Antigravity)

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
- [ ] **Fase 4: Magnet Links & DHT**: Parser de enlaces magnet (`magnet:?xt=...`), protocolo de metadatos `ut_metadata` ([BEP 0009](https://www.bittorrent.org/beps/bep_0009.html)) y Kademlia DHT ([BEP 0005](https://www.bittorrent.org/beps/bep_0005.html)).
- [ ] **Fase 5: Clientes & WebUI**: CLI nativo (`get-torrent-cli`) y Web Dashboard interactivo integrado en el daemon.

---

## 📄 Licencia

Distribuido bajo la Licencia MIT. Consulta `LICENSE` para más información.
