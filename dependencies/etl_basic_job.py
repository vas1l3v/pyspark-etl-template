import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from dependencies import logging


"""
Basic job class which initializes spark session, logger wrapper class
SparkSession is configured here
Spark configs are passed using configs/spark_config.json file
"""


class ETLBasicJob(object):
    spark = None
    logger = None

    def __init__(self, config):
        self.config = config

    def _get_spark_session(self):
        # if os.environ['ENV'] == 'dev':
        #     mode = 'local'
        # else:
        #     mode = 'yarn'

        spark_builder = SparkSession.builder \
            .master('local[*]') \
            .appName("Sample etl app")
        # add spark config params

        for key, val in self.config.items():
            spark_builder.config(key, val)

        # init session
        self.spark = spark_builder.getOrCreate()
        # set debug log level
        self.spark.sparkContext.setLogLevel("INFO")
        # init logger
        self.logger = logging.Log4j(self.spark)

        self.logger.info("spark session started")

    def _close_spark_session(self):
        self.logger.info("close spark session")
        if self.spark is not None:
            self.spark.stop()
        else:
            self.logger.warn('No such spark session')
