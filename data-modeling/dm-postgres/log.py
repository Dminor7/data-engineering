import logging

def get_logger(logger_name,file_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s|%(levelname)s|%(funcName)s|%(message)s')
    file_handler = logging.FileHandler(file_name)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger
    