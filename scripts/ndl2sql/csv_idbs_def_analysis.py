import argparse
from pathlib import Path
from shutil import rmtree
from pyspark.sql import SparkSession


def __main(spark: SparkSession):
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', dest='file', required=True, help='--file <file>',
                        type=str)
    parser.add_argument('-o', '--output-', dest='output', required=True, help='--output <output_folder>',
                        type=str)
    args = parser.parse_args()
    df = spark.read.csv(args.file, sep=',', inferSchema=True, header=True)
    df.show()

    if Path(args.output).exists():
        rmtree(args.output)

    df.coalesce(1).write.csv(args.output, header='true')


if __name__ == '__main__':
    spark_session = (
        SparkSession.builder
            .master("local[2]")
            .appName("DataTest")
            .config("spark.executorEnv.PYTHONHASHSEED", "0")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
    )
    __main(spark_session)
