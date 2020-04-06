from dependencies.etl_basic_job import ETLBasicJob
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, TimestampType, StringType, DoubleType\
    , BooleanType
from pyspark.sql.functions import *


class ETLSubmitJob(ETLBasicJob):
    def __init__(self, config):
        super(ETLSubmitJob, self).__init__(config)

    def start(self):
        try:

            # get spark session
            self._get_spark_session()

            self.logger.info("Job is starting")
            # TODO write code for the job

            # execute ETL pipeline
            raw_data = self._extract_data()
            transformed_data = self._transform_data(raw_data)
            transformed_data.show()
            self._load_data(transformed_data)

        except Exception:
            raise
        finally:
            # close spark session
            self._close_spark_session()

    def _extract_data(self):
        target_schema = self._get_target_schema()
        # no schema inference
        df = self.spark.read \
            .format("com.databricks.spark.csv") \
            .schema(target_schema) \
            .option("delimiter", ",") \
            .option("header", "true") \
            .option("dateFormat", "MMMM dd, yyyy") \
            .load("/source/fifa/players.2019.csv")
        #TO DO
        # get game_year from filename, do not leave it hardcoded
        df = df.withColumn("game_year", lit(2019))
        df.show()
        return df

    def _transform_data(self, df_raw):
        df_transformed = df_raw.select(col("ID")
                                       , col("Name")
                                       , col("Club")
                                       , col("Value")
                                       , col("Release_clause")
                                       , col("Nationality")
                                       , col("Overall")
                                       , col("Height")
                                       , col("Weight")
                                       ) \
                               .where(
                                    (col("ShotPower") > 80)
                                    & (col("Jumping") > 85)
                                    & (col("HeadingAccuracy") > 80)
                                    & (col("game_year") > col("game_year") - 10))
        return df_transformed

    def _load_data(self, df_to_load):
        # TO DO
        # Load to parquet
        self.logger.info("load data")

    @staticmethod
    def _get_target_schema():
        return StructType([
            StructField("row", IntegerType(), False),
            StructField("ID", IntegerType(), False),
            StructField("Name", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Photo", StringType(), True),
            StructField("Nationality", StringType(), True),
            StructField("Flag", StringType(), True),
            StructField("Overall", IntegerType(), True),
            StructField("Potential", IntegerType(), False),
            StructField("Club", StringType(), True),
            StructField("Club_logo", StringType(), True),
            StructField("Value", StringType(), True),
            StructField("Wage", StringType(), True),
            StructField("Special", IntegerType(), True),
            StructField("Preferred_foot", StringType(), True),
            StructField("International_reputation", IntegerType(), False),
            StructField("Weak_foot", IntegerType(), True),
            StructField("Skill_moves", IntegerType(), True),
            StructField("Work_rate", StringType(), True),
            StructField("Body_type", StringType(), True),
            StructField("Real_face", StringType(), True),
            StructField("Position", StringType(), True),
            StructField("Jersey_number", StringType(), False),
            StructField("Joined", DateType(), True),
            StructField("Loaned_from", StringType(), True),
            StructField("Contract_valid_until", StringType(), True),
            StructField("Height", StringType(), True),
            StructField("Weight", StringType(), True),
            StructField("LS", StringType(), True),
            StructField("ST", StringType(), False),
            StructField("RS", StringType(), True),
            StructField("LW", StringType(), True),
            StructField("LF", StringType(), True),
            StructField("CF", StringType(), False),
            StructField("RF", StringType(), True),
            StructField("RW", StringType(), True),
            StructField("LAM", StringType(), True),
            StructField("CAM", StringType(), False),
            StructField("RAM", StringType(), True),
            StructField("LM", StringType(), True),
            StructField("LCM", StringType(), True),
            StructField("CM", StringType(), True),
            StructField("RCM", StringType(), False),
            StructField("RM", StringType(), True),
            StructField("LWB", StringType(), True),
            StructField("LDM", StringType(), True),
            StructField("CDM", StringType(), True),
            StructField("RDM", StringType(), True),
            StructField("RWB", StringType(), False),
            StructField("LB", StringType(), True),
            StructField("LCB", StringType(), True),
            StructField("CB", StringType(), False),
            StructField("RCB", StringType(), True),
            StructField("RB", StringType(), True),
            StructField("Crossing", IntegerType(), True),
            StructField("Finishing", IntegerType(), True),
            StructField("HeadingAccuracy", IntegerType(), False),
            StructField("ShortPassing", IntegerType(), True),
            StructField("Volleys", IntegerType(), True),
            StructField("Dribbling", IntegerType(), True),
            StructField("Curve", IntegerType(), True),
            StructField("FKAccuracy", IntegerType(), False),
            StructField("Long_Passing", IntegerType(), True),
            StructField("BallControl", IntegerType(), True),
            StructField("Acceleration", IntegerType(), True),
            StructField("SprintSpeed", IntegerType(), True),
            StructField("Agility", IntegerType(), True),
            StructField("Reactions", IntegerType(), False),
            StructField("Balance", IntegerType(), True),
            StructField("ShotPower", IntegerType(), True),
            StructField("Jumping", IntegerType(), True),
            StructField("Stamina", IntegerType(), True),
            StructField("Strength", IntegerType(), True),
            StructField("LongShots", IntegerType(), True),
            StructField("Aggression", IntegerType(), False),
            StructField("Interceptions", IntegerType(), True),
            StructField("Positioning", IntegerType(), True),
            StructField("Vision", IntegerType(), True),
            StructField("Penalties", IntegerType(), True),
            StructField("Composure", IntegerType(), False),
            StructField("Marking", IntegerType(), True),
            StructField("StandingTackle", IntegerType(), True),
            StructField("SlidingTackle", IntegerType(), True),
            StructField("GKDiving", IntegerType(), True),
            StructField("GKHandling", IntegerType(), True),
            StructField("GKKicking", IntegerType(), False),
            StructField("GKPositioning", IntegerType(), True),
            StructField("GKReflexes", IntegerType(), True),
            StructField("Release_clause", StringType(), True)
        ])
