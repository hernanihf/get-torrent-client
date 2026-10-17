---
name: torrent-analyst
description: >-
  Use this skill when analyzing BitTorrent protocols (BEPs), Transmission RPC specs,
  network architectures, peer wire state machines, and breaking down requirements into acceptance criteria.
---

# Torrent Protocol & Requirements Analyst Skill

Esta skill guía al agente analista en la especificación de componentes para el cliente BitTorrent `get-torrent`.

## Pautas de Análisis para BitTorrent

1. **Revisión de Especificaciones Estándar**:
   - **BEP 0003**: Metainfo, Bencode (`i<integer>e`, `<length>:<string>`, `l<list>e`, `d<dict>e`), Handshake P2P (`<pstrlen><pstr><reserved><info_hash><peer_id>`), Tipos de mensajes (0: choke, 1: unchoke, 2: interested, 3: not interested, 4: have, 5: bitfield, 6: request, 7: piece, 8: cancel).
   - **BEP 0015**: UDP Tracker Protocol (Connect request, Announce request, Connection ID).
   - **BEP 0009 / BEP 0010**: Extension protocol para Magnet links (`ut_metadata`).
   - **Transmission RPC**: Métodos JSON-RPC como `torrent-get`, `torrent-add`, `session-get`, `torrent-start`, `torrent-stop`.

2. **Formato de la Especificación de Salida**:
   ```markdown
   ### 📋 Especificación Técnica: [Nombre de la Funcionalidad / BEP]

   #### 1. Resumen y Alcance
   [Objetivo dentro del cliente get-torrent]

   #### 2. Formato de Datos / Protocolo
   - Estructura de paquetes binarios / Frames / JSON
   - Estados de conexión (Choked/Unchoked, Interested/Not Interested)

   #### 3. Criterios de Aceptación
   - Scenario 1: [Given ... When ... Then ...]
   - Scenario 2: [Given ... When ... Then ...]

   #### 4. Archivos a Crear o Modificar
   - `pkg/...` o `src/...`

   #### 5. Riesgos y Casos Borde
   - [Pérdida de paquetes, datos corruptos, timeouts, peers maliciosos]
   ```

3. **Restricción**:
   - El Analista trabaja en modo solo lectura (`enable_write_tools: false`). Su salida es la especificación técnica para el Coder.
