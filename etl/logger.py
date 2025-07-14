#!/usr/bin/env python3
import os
import sys
import uuid
import logging
from logging.handlers import TimedRotatingFileHandler
from pythonjsonlogger import jsonlogger
from logdna import LogDNAHandler

import config


def setup_logger(service_name: str) -> logging.LoggerAdapter:
    """
    Configure and return a JSON-formatted LoggerAdapter for services.

    Adds Console, File, and optional LogDNA (Mezmo) handlers.
    Ensures handlers are only added once to avoid duplicates.
    """
    # Determine log level and correlation ID
    log_level = config.LOG_LEVEL
    correlation_id = config.CORRELATION_ID or uuid.uuid4().hex

    # Base logger
    logger = logging.getLogger(service_name)
    if not logger.handlers:
        logger.setLevel(log_level)
        logger.propagate = False

        # JSON formatter
        json_formatter = jsonlogger.JsonFormatter(
            '%(timestamp)s %(service)s %(level)s %(message)s %(correlation_id)s',
            rename_fields={'asctime': 'timestamp', 'levelname': 'level'}
        )

        # Console stdout
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(log_level)
        stream_handler.setFormatter(json_formatter)
        logger.addHandler(stream_handler)

        # File with daily rotation
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

        # Optional: LogDNA/Mezmo handler if key present
        mezmo_key = os.getenv('MEZMO_KEY')
        if mezmo_key:
            mezmo_opts = {
                'hostname': os.getenv('HOSTNAME', service_name),
                'app': service_name,
                'index_meta': True
            }
            mezmo_handler = LogDNAHandler(key=mezmo_key, options=mezmo_opts)
            mezmo_handler.setLevel(log_level)
            mezmo_handler.setFormatter(json_formatter)
            logger.addHandler(mezmo_handler)

        # Capture Python warnings
        logging.captureWarnings(True)

    # Adapter for run-specific metadata
    adapter = logging.LoggerAdapter(logger, {
        'service': service_name,
        'correlation_id': correlation_id
    })

    # Global exception hook
    if not hasattr(logger, '_exception_hooked'):
        def handle_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            adapter.error('Unhandled exception', exc_info=(exc_type, exc_value, exc_traceback))
        sys.excepthook = handle_exception
        setattr(logger, '_exception_hooked', True)

    return adapter
