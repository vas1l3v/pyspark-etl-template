"""
dependencies etc...
"""
import json
import os

from dependencies.etl_submit_job import ETLSubmitJob
from pathlib import Path


def main():
    """Main ETL script definition.
    :return: None
    """
    # with open(str(Path(os.getcwd()).parent) + '/configs/spark_config.json') as conf_file:

    with open(
            str(Path(os.getcwd()).parent) + '/configs/spark_config.json') as conf_file:
        config = json.load(conf_file)

    etl_job = ETLSubmitJob(config)

    etl_job.start()

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
