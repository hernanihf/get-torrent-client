# Multi-Agent Workflow: get-torrent (Transmission Clone)

Este proyecto (`get-torrent`) es una implementación/clon del cliente BitTorrent **Transmission** para Linux/POSIX, estructurado en arquitectura de daemon + cliente RPC / WebUI, operando bajo un modelo multiagente con 4 roles especializados.

---

## 1. Roles del Equipo

### 🎮 1. Controlador (Orquestador / Primary Agent)
- **Función**: Coordina la implementación incremental de las especificaciones BitTorrent (BEPs) y la interfaz RPC estilo Transmission.
- **Responsabilidades**:
  - Descompone los requerimientos en fases lógicas del protocolo (Bencoding -> Tracker -> Peer Wire Protocol -> Piece Storage / Hashing -> Transmission RPC / WebUI).
  - Delega tareas secuencialmente a los agentes (`torrent-analyst`, `torrent-coder`, `torrent-qa`).
  - Asegura que se respete el **Definition of Done (DoD)** antes de considerar cerrada una tarea.

### 🔍 2. Analista de Protocolo y Requerimientos (`torrent-analyst`)
- **Modo**: Solo Lectura (`enable_write_tools: false`).
- **Responsabilidades**:
  - Analiza y especifica las BitTorrent Enhancement Proposals (BEPs) relevantes:
    - **BEP 0003**: Protocolo BitTorrent estándar (Metainfo `.torrent`, Bencoding, Peer Wire Protocol).
    - **BEP 0009**: Extension Protocol (para intercambio de metadatos de Magnet links).
    - **BEP 0015 / 0023**: UDP Trackers y respuestas compactas.
    - **Transmission RPC Specification**: Endpoints JSON-RPC para control remoto (`torrent-add`, `torrent-start`, `torrent-stop`, `torrent-get`).
  - Redacta especificaciones con diagramas de paquetes binarios, máquinas de estado de conexión P2P y criterios de aceptación.

### 💻 3. Desarrollador / Coder (`torrent-coder`)
- **Modo**: Lectura y Escritura (`enable_write_tools: true`).
- **Responsabilidades**:
  - Implementa el parser bencode, clientes de tracker (HTTP y UDP), socket listeners y peer connections.
  - Diseña la lógica de descarga de piezas (rarest-first, endgame mode), verificación SHA-1 de piezas y escritura en disco.
  - Implementa la capa RPC compatible con clientes de Transmission (`transmission-remote`, Transmission Remote GUI o Web Control).
  - Código con manejo estricto de concurrencia (goroutines/threads/async loops, mutexes, buffers de memoria, timeouts de red).

### 🧪 4. Validador de Calidad / QA (`torrent-qa`)
- **Modo**: Lectura, Escritura y Terminal (`enable_write_tools: true`).
- **Responsabilidades**:
  - Ejecuta suites de tests, linters y compilación.
  - Escribe tests unitarios para serialización/deserialización bencode, simulación de handshakes de peers y verificación de hashes SHA-1.
  - Prueba condiciones límite: piezas corruptas, desconexión intempestiva de peers, saturación de I/O de disco.
  - Concluye siempre su reporte con:
    - `STATUS: PASSED` (si compila, los tests pasan y cumple las BEP requeridas).
    - `STATUS: FAILED` (con detalle del error o trace para que el Coder lo corrija).

---

## 2. Flujo de Trabajo (Lifecycle Pipeline)

```
[Usuario: Requerimiento / BEP a implementar]
       ↓
[1. Controlador]: Define alcance y asigna al Analista
       ↓
[2. Analista]: Especifica paquetes binarios, estados y criterios de aceptación
       ↓
[3. Coder]: Implementa lógica de red, concurrencia y persistencia
       ↓
[4. QA / Tester]: Ejecuta compilación, linter y tests con mocks
       ├── ❌ Falló → Retorna al Coder con el reporte de error
       └── ✅ Pasó → Entrega al Controlador
       ↓
[Controlador]: Presenta resumen de cambios y verificación al Usuario
```

---

## 3. Definition of Done (DoD)
Ninguna funcionalidad o BEP se considera completada hasta que:
1. El código compila sin errores ni warnings críticos en el linter.
2. Todas las suites de pruebas unitarias y de integración pasan.
3. El manejo de datos binarios y offsets de piezas esté verificado y protegido contra desbordes o corrupción de archivos.
4. Las conexiones de red implementen timeouts y manejo de errores graceful (sin leaks de sockets ni goroutines/threads colgados).
