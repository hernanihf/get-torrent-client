---
name: torrent-coder
description: >-
  Use this skill when implementing network protocols, bencoding parsers, peer wire framing,
  disk storage managers, or Transmission RPC interfaces in get-torrent.
---

# Torrent Coder Skill

Esta skill define las pautas de desarrollo para el agente programador en `get-torrent`.

## Principios de Implementación

1. **Eficiencia en Red e I/O**:
   - Usar buffers de tamaño controlado para descarga de bloques (típicamente 16 KB por bloque dentro de cada pieza).
   - Verificación estricta de hashes SHA-1 para cada pieza antes de marcarla como disponible (`have`).
   - Gestión adecuada de sockets TCP/UDP con timeouts de lectura y escritura para evitar bloqueos por peers caídos.

2. **Seguridad en Concurrencia**:
   - Proteger el estado de las piezas y la lista de peers con mecanismos adecuados (mutexes, channels, locks o event-loops).
   - Evitar condiciones de carrera al solicitar bloques concurrentemente a múltiples peers.

3. **Compatibilidad con Transmission**:
   - Los endpoints RPC deben soportar la autenticación de sesión `X-Transmission-Session-Id` y las firmas de métodos estándar de Transmission para permitir el uso de interfaces gráficas existentes como Transmission Remote GUI.

4. **Entrega al Agente de QA**:
   - Listar los módulos modificados, comandos para compilar y ejecutar los tests correspondientes.
