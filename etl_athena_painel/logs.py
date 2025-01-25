import logging.config
from etl_athena_painel.colorlogs import ColorLogg  # noqa: F401

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "()": "etl_athena_painel.colorlogs.ColorLogg",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%d/%m/%Y %H:%M:%S",
        },
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
            "datefmt": "%d/%m/%Y %H:%M:%S",
        },
    },
    "handlers": {
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "maxBytes": 20 * 1024 * 1024,
            "filename": "etl.log",
            "backupCount": 10,
            "encoding": "utf-8",
        },
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
        },
    },
    "loggers": {
        "": {
            "level": "INFO",
            "handlers": ["file", "console"],
        },
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("etl_painel")
