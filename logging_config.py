import logging
import sys
import structlog
from structlog.types import Processor

def setup_logging():
    """
    Configura structlog para la aplicación.
    - Usa un renderizador de consola para desarrollo si se ejecuta en un TTY.
    - Usa un renderizador JSON para producción.
    """
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # Determinar el renderizador basado en si la salida es una terminal
    if sys.stdout.isatty():
        # Renderizador para desarrollo (legible en consola)
        final_processor = structlog.dev.ConsoleRenderer()
    else:
        # Renderizador para producción (JSON)
        final_processor = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Configurar el formateador para el logger raíz de Python
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=final_processor,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    # Silenciar logs de s3fs que son muy verbosos en nivel INFO
    logging.getLogger("s3fs").setLevel(logging.WARNING)
    logging.getLogger("fsspec").setLevel(logging.WARNING)

    print("Logging configurado con structlog.")

