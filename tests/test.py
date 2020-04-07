import unittest
import json
from pyspark.sql.functions import *
from dependencies.etl_submit_job import ETLSubmitJob


class ETLTests(unittest.TestCase, ETLSubmitJob):

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        # hardcode config file for test
        with open(
                "C:\\Users\\pvasi\\OneDrive\\Desktop\\Big Data\\pyspark-project-template\\configs\\spark_config.json") as conf_file:
            self.config = json.load(conf_file)

        # path to unit test file in hdfs
        self.test_file_path = "/unittests/players.2019.csv"
        self.test_file_expected = '/unittest/fifa_expected.csv'
        # init spark session
        self._get_spark_session()

        # prepare actual data
        input_raw_data = self._extract_data(self.test_file_path)
        self.input_transformed_data = self._transform_data(input_raw_data)

        # prepare expected data
        self.expected_data = self.spark \
                                .read \
                                .csv(self.test_file_expected)

    def tearDown(self):
        """Stop Spark
        """
        self._close_spark_session()

    # test to check for duplicates on player id

    def test_data_integrity(self):

        duplicates_count = self.input_transformed_data.groupby("ID") \
                                .agg(count("ID").alias("count_dupl")) \
                                .filter(col("count_dupl") > 1) \
                                .count()
        self.assertEqual(duplicates_count, 0)

    # test to check if target count matches expected count

    def test_record_count(self):
        record_count = self.input_transformed_data.count()
        self.assertEqual(record_count, 1)

    # test to check if column transformations are correct

    def test_except_df(self):
        df_expected_diff = self.expected_data.exceptAll(self.input_transformed_data).count()
        df_actual_diff = self.input_transformed_data.exceptAll(self.expected_data).count()
        self.assertEqual(df_actual_diff + df_expected_diff, 0)


if __name__ == '__main__':
    unittest.main()
