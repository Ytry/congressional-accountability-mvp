#!/usr/bin/env python3
import os
import sys
import uuid
import logging
from logging.handlers import TimedRotatingFileHandler
from pythonjsonlogger import jsonlogger
from logdna import LogDNAHandler

import config


class AIPromptFormatter(logging.Formatter):
    """
    Wraps any log record into an 'AI_PROMPT' block that
    asks an LLM to suggest fixes for this log entry.
    """
    def format(self, record):
        # First, get our JSON-formatted log
        original = super().format(record)
        # Now wrap it as a prompt
        prompt = (
            "AI_PROMPT:\n"
            f"I have a log entry from service '{getattr(record, 'service', '')}'\n"
            f"Level: {record.levelname}\n"
            "Here is the full JSON log:\n"
            f"{original}\n\n"
            "Please propose specific code or config changes to address this issue.\n"
        )
        return prompt


def setup_logger(service_name: str) -> logging.LoggerAdapter:
    log_level      = config.LOG_LEVEL
    correlation_id = config.CORRELATION_ID or uuid.uuid4().hex

    logger = logging.getLogger(service_name)
    if not logger.handlers:
        logger.setLevel(log_level)
        logger.propagate = False

        # ─── JSON Formatter ─────────────────────────────────────
        json_fmt = jsonlogger.JsonFormatter(
            '%(timestamp)s %(service)s %(level)s %(message)s %(correlation_id)s',
            rename_fields={'asctime': 'timestamp', 'levelname': 'level'}
        )

        # ─── Console Handler ────────────────────────────────────
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(log_level)
        sh.setFormatter(json_fmt)
        logger.addHandler(sh)

        # ─── Rotating File Handler ──────────────────────────────
        log_dir = config.LOGS_DIR
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = TimedRotatingFileHandler(
            filename=str(log_dir / f"{service_name}.log"),
            when="midnight",
            backupCount=14,
            encoding='utf-8'
        )
        fh.setLevel(log_level)
        fh.setFormatter(json_fmt)
        logger.addHandler(fh)

        # ─── Mezmo (LogDNA) Handler ────────────────────────────
        mezmo_key = os.getenv('MEZMO_KEY')
        if mezmo_key:
            mezmo_opts = {
                'hostname': os.getenv('HOSTNAME', service_name),
                'app': service_name,
                'index_meta': True
            }
            lh = LogDNAHandler(key=mezmo_key, options=mezmo_opts)
            lh.setLevel(log_level)
            lh.setFormatter(json_fmt)
            logger.addHandler(lh)

        # ─── AI Prompt Handler ─────────────────────────────────
        # Always emit an INFO-level prompt after every log
        aip = logging.StreamHandler(sys.stdout)
        aip.setLevel(logging.INFO)
        # Use the same JSON formatter as base, but wrap via AIPromptFormatter
        base_fmt = jsonlogger.JsonFormatter(
            '%(timestamp)s %(service)s %(level)s %(message)s %(correlation_id)s',
            rename_fields={'asctime': 'timestamp', 'levelname': 'level'}
        )
        # We want the raw JSON for embedding in the prompt, so configure parent
        parent = logging.Formatter()
        parent.format = lambda record: base_fmt.format(record)
        prompt_fmt = AIPromptFormatter()
        # But AIPromptFormatter inherits from Formatter, so we override its format call
        AIPromptFormatter.__bases__ = (logging.Formatter,)
        AIPromptFormatter.format = AIPromptFormatter.format
        aip.setFormatter(prompt_fmt)
        logger.addHandler(aip)

        # Capture Python warnings
        logging.captureWarnings(True)

    # Attach metadata via Adapter
    adapter = logging.LoggerAdapter(logger, {
        'service':        service_name,
        'correlation_id': correlation_id
    })

    # Global exception hook
    if not hasattr(logger, '_exception_hooked'):
        def handle_exception(exc_type, exc_val, exc_tb):
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_val, exc_tb)
                return
            adapter.error('Unhandled exception', 
                          exc_info=(exc_type, exc_val, exc_tb))
        sys.excepthook = handle_exception
        setattr(logger, '_exception_hooked', True)

    return adapter
