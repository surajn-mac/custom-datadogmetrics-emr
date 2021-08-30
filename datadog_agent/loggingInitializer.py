import logging


class initialize_logger:
    def __init__(self, output_dir):
        self.output_dir = output_dir

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        # create console handler and set level to info
        handler = logging.StreamHandler()
        # handler.setLevel(logging.INFO)
        # log_file_format = "[%(levelname)s] - %(asctime)s - %(name)s - %(pathname)s:%(lineno)d : %(message)s"
        log_file_format = "[%(levelname)s] - %(asctime)s - %(name)s - %(pathname)s - %(module)s:%(funcName)s:%(lineno)d : %(message)s"
        formatter = logging.Formatter(log_file_format)
        handler.setFormatter(formatter)
        if not self.logger.hasHandlers():
            self.logger.addHandler(handler)

        # Custom Levels
        logging.addLevelName(15, "MORE INFO")

        # create error file handler and set level to error
        # handler = logging.FileHandler(os.path.join(output_dir, "error.log"), "w", encoding=None, delay="true")
        # handler.setLevel(logging.ERROR)
        # formatter = logging.Formatter(log_file_format)
        # handler.setFormatter(formatter)
        # logger.addHandler(handler)
        #
        # # create debug file handler and set level to debug
        # handler = logging.FileHandler(os.path.join(output_dir, "all.log"), "w")
        # handler.setLevel(logging.DEBUG)
        # formatter = logging.Formatter(log_file_format)
        # handler.setFormatter(formatter)
        # logger.addHandler(handler)

    def info(self, message):
        self.logger.info(message)

    def warn(self, message):
        self.logger.warn(message)

    def debug(self, message):
        self.logger.debug(message)

    def more_info(self, message):
        self.logger.log(15, message)

    def setLoggerLevel(self, level):
        self.logger.setLevel(level)
