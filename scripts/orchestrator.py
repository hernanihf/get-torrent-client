#!/usr/bin/env python3
"""
Orquestador Multiagente para get-torrent (Transmission Clone) utilizando el SDK de Google Antigravity.
Ejecuta secuencialmente: Analista de Protocolo -> Coder de Red/P2P -> QA Tester con bucle de autocorrección.

Requisito previo:
    pip install google-antigravity
Uso:
    python scripts/orchestrator.py "Implementar decodificador bencode para enteros y strings"
"""

import asyncio
import sys
import os
from google.antigravity import Agent, LocalAgentConfig, CapabilitiesConfig

analyst_config = LocalAgentConfig(
    system_instructions=(
        "Eres el Analista de Protocolo y Arquitecto de Software de get-torrent "
        "(un clon modular de Transmission BitTorrent en Linux). "
        "Inspecciona el codebase en modo solo lectura. Ante un requerimiento del usuario, "
        "diseña la especificación técnica basada en las BEPs oficiales de BitTorrent y la RPC de Transmission:\n"
        "1. Estructura de paquetes binarios / mensajes de protocolo / endpoints RPC\n"
        "2. Criterios de Aceptación (Given/When/Then)\n"
        "3. Archivos exactos a crear o modificar\n"
        "4. Manejo de errores y condiciones límite (timeouts, datos corruptos, desconexiones).\n"
        "No intentes modificar archivos."
    ),
    # Solo lectura
)

coder_config = LocalAgentConfig(
    system_instructions=(
        "Eres el Programador Senior de Redes y Sistemas en get-torrent. "
        "Recibes la especificación técnica del Analista y desarrollas el código necesario. "
        "Pon especial atención al manejo eficiente de memoria, buffers binarios, big-endian byte order, "
        "seguridad en concurrencia y compatibilidad con el estándar BitTorrent / Transmission RPC."
    ),
    capabilities=CapabilitiesConfig(), # Habilita herramientas de edición de archivos
)

tester_config = LocalAgentConfig(
    system_instructions=(
        "Eres el Ingeniero de QA y Testing de get-torrent. "
        "Tu tarea es validar la implementación corriendo los tests y linters en la terminal. "
        "Verifica que el código compile, que los tests unitarios de paquetes/bencode pasen, "
        "y que se cumplan las especificaciones. "
        "Finaliza tu respuesta con 'STATUS: PASSED' si todo es correcto, "
        "o 'STATUS: FAILED' con el detalle del error para que el Coder lo corrija."
    ),
    capabilities=CapabilitiesConfig(), # Habilita ejecución de comandos en terminal
)

async def run_pipeline(requirement: str, max_retries: int = 3):
    cwd = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    print(f"🎯 Iniciando Pipeline Multiagente en: {cwd}")
    print(f"📝 Requerimiento: {requirement}\n" + "="*60)

    # 1. ANÁLISIS DE PROTOCOLO
    print("\n🔍 [Paso 1/3] Analista de Protocolo estudiando requerimientos y BEPs...")
    async with Agent(analyst_config) as analyst:
        response = await analyst.chat(
            f"El usuario solicita la siguiente funcionalidad para get-torrent:\n\n{requirement}\n\n"
            "Por favor, elabora la especificación técnica detallada."
        )
        spec = "".join([token async for token in response])
        print("\n📋 Especificación de protocolo generada con éxito.")

    # 2. IMPLEMENTACIÓN Y TESTING EN BUCLE (Feedback Loop)
    success = False
    current_feedback = ""

    for attempt in range(1, max_retries + 1):
        print(f"\n💻 [Paso 2/3] Coder implementando protocolo y lógica (Intento {attempt}/{max_retries})...")
        async with Agent(coder_config) as coder:
            prompt = f"Especificación técnica aprobada:\n\n{spec}\n"
            if current_feedback:
                prompt += (
                    f"\n\n⚠️ ATENCIÓN: El intento anterior falló la validación con este reporte:\n"
                    f"{current_feedback}\n"
                    "Por favor, revisa y corrige los errores."
                )
            response = await coder.chat(prompt)
            async for token in response:
                pass

        print(f"\n🧪 [Paso 3/3] QA ejecutando pruebas y validaciones...")
        async with Agent(tester_config) as tester:
            response = await tester.chat(
                f"Valida los cambios recientes contra esta especificación:\n\n{spec}\n\n"
                "Ejecuta las pruebas y verificaciones necesarias en el proyecto.\n"
                "Termina obligatoriamente con STATUS: PASSED o STATUS: FAILED."
            )
            report = "".join([token async for token in response])

            if "STATUS: PASSED" in report:
                print("\n🎉 ¡Validación de QA exitosa! Protocolo y tests verificados.")
                success = True
                break
            else:
                print(f"\n⚠️ QA detectó fallos en el intento {attempt}.")
                current_feedback = report

    if not success:
        print("\n❌ Se alcanzó el número máximo de reintentos sin lograr una validación limpia.")

if __name__ == "__main__":
    req = sys.argv[1] if len(sys.argv) > 1 else "Implementar parser y serializador Bencode básico (enteros y strings)."
    asyncio.run(run_pipeline(req))
