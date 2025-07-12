import logging
import sys
import uuid
from logging.handlers import TimedRotatingFileHandler
from pythonjsonlogger import jsonlogger
import config


def setup_logger(service_name: str) -> logging.LoggerAdapter:
    """
    Configure and return a JSON-formatted LoggerAdapter for ETL scripts.

    Uses configuration from config.py (LOG_LEVEL, CORRELATION_ID, LOGS_DIR),
    ensures handlers are only added once to avoid duplicates.
    """
    # Determine log level and correlation ID
    log_level = config.LOG_LEVEL
    correlation_id = config.CORRELATION_ID or uuid.uuid4().hex

    # Get or create the base logger
    logger = logging.getLogger(service_name)

    # One-time handler setup
    if not logger.handlers:
        logger.setLevel(log_level)
        logger.propagate = False

        # JSON formatter with renamed fields for consistency
        json_formatter = jsonlogger.JsonFormatter(
            '%(timestamp)s %(service)s %(level)s %(message)s %(correlation_id)s',
            rename_fields={
                'asctime': 'timestamp',
                'levelname': 'level'
            }
        )

        # Console (stdout) handler
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(log_level)
        stream_handler.setFormatter(json_formatter)
        logger.addHandler(stream_handler)

        # File handler with daily rotation
        log_dir = config.LOGS_DIR
        log_dir.mkdir(parents=True, exist_ok=True)
        file_path = log_dir / f"{service_name}.log"
        file_handler = TimedRotatingFileHandler(
            filename=str(file_path),
            when="midnight",
            backupCount=14,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(json_formatter)
        logger.addHandler(file_handler)

        # Capture Python warnings via logging
        logging.captureWarnings(True)

    # Create adapter with run-specific metadata
    adapter = logging.LoggerAdapter(logger, {
        'service': service_name,
        'correlation_id': correlation_id
    })

    # One-time exception hook
    if not hasattr(logger, '_exception_hooked'):
        def handle_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                # Default behavior for Ctrl-C
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            adapter.error('Unhandled exception', exc_info=(exc_type, exc_value, exc_traceback))

        sys.excepthook = handle_exception
        setattr(logger, '_exception_hooked', True)

    return adapter
