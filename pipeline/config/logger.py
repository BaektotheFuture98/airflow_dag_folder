import logging
from typing import Optional


# DEFAULT_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
DEFAULT_FORMAT = "%(name)s | %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
	"""Return a configured logger with a standard console handler.

	- Format: time | level | logger name | message
	- Date format: YYYY-MM-DD HH:MM:SS
	- Level default: INFO
	"""
	logger = logging.getLogger(name or __name__)
	logger.setLevel(level)

	# Avoid duplicate handlers if get_logger is called multiple times
	if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
		handler = logging.StreamHandler()
		handler.setLevel(level)
		formatter = logging.Formatter(fmt=DEFAULT_FORMAT, datefmt=DEFAULT_DATEFMT)
		handler.setFormatter(formatter)
		logger.addHandler(handler)

	# Optional: propagate to root? Keep False for clean console output in Airflow tasks
	logger.propagate = False
	return logger

