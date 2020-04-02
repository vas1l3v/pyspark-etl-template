"""
Wrapper class for pyspark logging
"""


class Log4j(object):

    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(__name__)

    def error(self, message):
        self.logger.error(message)
        return None

    def warn(self, message):
        self.logger.warn(message)
        return None

    def info(self, message):
        self.logger.info(message)
        return None
