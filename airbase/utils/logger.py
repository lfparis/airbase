import logging


class Logger(object):
    @staticmethod
    def start(name, level="info"):
        LOG_FORMAT = "%(asctime)s (%(name)s): %(levelname)s - %(message)s"
        DATE_FORMAT = "%Y-%m-%d %H:%M:%S (UTC/GMT %z)"

        logger = logging.getLogger(name)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
        handler.setFormatter(formatter)

        Logger.set_level(logger, level)

        logger.addHandler(handler)
        logger.propagate = False

        return logger

    @staticmethod
    def set_level(logger, level):
        level = level.upper()
        if isinstance(level, str) and level in logging._nameToLevel:
            level = getattr(logging, level)
            logger.setLevel(level)


if __name__ == "__main__":
    logger = Logger.start("foo")
    logger.info("bar")
    Logger.set_level(logger, "warning")
