# get-torrent

`get-torrent` es una reimplementación ligera y modular inspirada en **Transmission**, el popular cliente BitTorrent para Linux. 

Diseñado con una arquitectura desacoplada en:
- **Daemon Core**: Motor BitTorrent para descarga de archivos, manejo de bencoding, comunicación con trackers y protocolo P2P.
- **Transmission RPC**: Capa de API JSON-RPC compatible con el protocolo de Transmission para permitir el uso de clientes remotos (`transmission-remote`, WebUI, Transmission Remote GUI).
- **Web / CLI Interface**: Interfaz de control simple y rápida.

---

## Arquitectura Multiagente

El desarrollo de este proyecto se realiza bajo el modelo multiagente de **Google Antigravity**:
- **Controlador (Primary Agent)**: Orquestador general del ciclo de vida y asignación de tareas.
- **Analista de Protocolo (`torrent-analyst`)**: Especificación técnica de BEPs (BitTorrent Enhancement Proposals) y RPC.
- **Desarrollador (`torrent-coder`)**: Implementación de networking, concurrencia, serialización y almacenamiento.
- **QA & Testing (`torrent-qa`)**: Validación de compilación, linter, pruebas de protocolo y simulación de red.

Consulta [AGENTS.md](AGENTS.md) para más detalles.

---

## Roadmap

1. [ ] **Fase 1: Bencoding & Metainfo**: Parser y serializer de Bencode + decodificación de archivos `.torrent`.
2. [ ] **Fase 2: Tracker Communication**: Anuncios HTTP y UDP (BEP 15) para obtención de lista de peers.
3. [ ] **Fase 3: Peer Wire Protocol**: Handshake P2P, intercambio de bitfields y descarga de piezas por bloques (16 KB).
4. [ ] **Fase 4: Storage & Integrity**: Reensamblado de archivos y verificación criptográfica SHA-1.
5. [ ] **Fase 5: Transmission RPC Server**: Endpoints JSON-RPC compatibles para control remoto.
6. [ ] **Fase 6: Web Dashboard**: Interfaz web mínima para monitorear descargas.
