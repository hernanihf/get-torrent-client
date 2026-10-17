---
name: torrent-qa
description: >-
  Use this skill when validating code changes in get-torrent, running test suites,
  mocking network packets/trackers, verifying bencoding, and ensuring protocol compliance.
---

# Torrent QA & Validator Skill

Esta skill define el procedimiento de control de calidad para `get-torrent`.

## Procedimiento de Validación

1. **Chequeo de Integridad y Compilación**:
   - Compilar el proyecto y verificar que no existan errores de tipos o sintaxis.
   - Ejecutar el linter del lenguaje correspondiente.

2. **Validación de Protocolo**:
   - Pruebas unitarias de Bencoding (codificación y decodificación de enteros, cadenas con prefijo, listas y diccionarios ordenados).
   - Pruebas de cálculo de `info_hash` con torrents de referencia conocidos.
   - Pruebas de framing de paquetes peer wire (longitud de 4 bytes en big-endian + ID de mensaje).

3. **Pruebas de Casos Extremos**:
   - Simular trackers que devuelven errores o respuestas compactas/no-compactas.
   - Simular piezas corruptas para asegurar que se descartan y se vuelven a solicitar.
   - Validar que el servidor RPC rechace peticiones sin header `X-Transmission-Session-Id` retornando código 409 según la especificación de Transmission.

4. **Veredicto Final**:
   El agente debe concluir con:

   ```markdown
   ### ✅ Verificación de QA
   - Compilación: PASSED
   - Linter: PASSED
   - Pruebas Unitarias / Protocolo: PASSED
   - Criterios de Aceptación: CUMPLIDOS
   STATUS: PASSED
   ```

   O en caso de error:
   ```markdown
   ### ❌ Fallos Detectados
   [Detalle exacto del error o test fallido]
   STATUS: FAILED
   ```
