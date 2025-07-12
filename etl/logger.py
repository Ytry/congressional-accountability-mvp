import logging
import os
import sys
import uuid
from logging.handlers import TimedRotatingFileHandler
from pythonjsonlogger import jsonlogger


def setup_logger(service_name: str) -> logging.LoggerAdapter:
    """
    Configure and return a JSON-formatted LoggerAdapter for ETL scripts.

    Features:
    - Daily rotating file logs (retains 14 days)
    - Console (stdout) logs in JSON
    - Correlation ID from env or generated per run
    - Unhandled exception capture
    - Standardized fields: timestamp, service, level, message, correlation_id
    """
    # Determine log level and correlation ID
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    correlation_id = os.getenv("CORRELATION_ID") or uuid.uuid4().hex

    # Base logger
    logger = logging.getLogger(service_name)
    logger.setLevel(log_level)
    logger.propagate = False

    # JSON formatter with renamed fields
    json_formatter = jsonlogger.JsonFormatter(
        '%(timestamp)s %(service)s %(level)s %(message)s %(correlation_id)s',
        rename_fields={
            'asctime': 'timestamp',
            'levelname': 'level'
        }
    )

    # Stream handler -> stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(log_level)
    stream_handler.setFormatter(json_formatter)
    logger.addHandler(stream_handler)

    # Ensure logs directory exists
    logs_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logs_dir, exist_ok=True)

    # Timed rotating file handler -> logs/<service_name>.log
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(logs_dir, f"{service_name}.log"),
        when="midnight",
        backupCount=14,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(json_formatter)
    logger.addHandler(file_handler)

    # Capture warnings from the warnings module
    logging.captureWarnings(True)

    # Create adapter with default metadata
    adapter = logging.LoggerAdapter(logger, {
        'service': service_name,
        'correlation_id': correlation_id
    })

    # Hook unhandled exceptions
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            # Call default handler for KeyboardInterrupt
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        adapter.error('Unhandled exception', exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception

    return adapter
