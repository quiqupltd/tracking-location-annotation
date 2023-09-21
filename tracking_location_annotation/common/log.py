"""
Define logger configuration
"""
import logging

from tracking_location_annotation.common.constants import LOG_LEVEL

# silent urllib3 and google libraries DEBUG logs
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("google").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    "Returns a logger with the given name."

    logging.basicConfig(level=LOG_LEVEL, format="%(name)-12s: %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # get logger instance
    logger = logging.getLogger(name)
    # define a Handler which writes messages the sys.stderr
    file = logging.FileHandler(filename="app.log", mode="w")
    file.setLevel(LOG_LEVEL)
    # set a format which is better for file use
    formatter = logging.Formatter(
        "%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    )
    # tell the handler to use this format
    file.setFormatter(formatter)
    # add the handler to the root logger
    # logger.addHandler(file)
    return logger
