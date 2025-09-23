import logging
from logging.handlers import RotatingFileHandler
import os

def get_logger(name: str = __name__,
               level: str = None,
               log_dir: str = "logs",
               filename: str = "app.log") -> logging.Logger:
    """Return a configured logger with console + rotating file handlers."""
    logger = logging.getLogger(name)
    if logger.handlers:  # évite double configuration si déjà créé
        return logger

    # Niveau via ENV (LOG_LEVEL=DEBUG/INFO/WARNING/ERROR)
    level_name = level or os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(level_name)

    # Formats
    fmt = "%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    # Console
    ch = logging.StreamHandler()
    ch.setLevel(level_name)
    ch.setFormatter(formatter)

    # Fichier rotatif
    os.makedirs(log_dir, exist_ok=True)
    fh = RotatingFileHandler(
        os.path.join(log_dir, filename),
        maxBytes=2_000_000,  # ~2MB
        backupCount=5,       # garde 5 archives
        encoding="utf-8"
    )
    fh.setLevel(level_name)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.propagate = False
    return logger
