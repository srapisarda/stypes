import argparse
import re
from pathlib import Path
from shutil import rmtree
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_replace, regexp_extract, avg, round


def __main(spark: SparkSession):
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--folder-path', dest='folder_path', required=True, help='--folder-path <folder_path>',
                        type=str)
    args = parser.parse_args()
    csv_file_path = re.search('([^\/]+$)', args.folder_path).group(0)
    csv_file_path = f'{args.folder_path}/{csv_file_path}_analysis'

    if Path(csv_file_path).exists():
        rmtree(csv_file_path)

    df = spark.read.option("pathGlobFilter", "*_eval.csv") \
        .option("recursiveFileLookup", "true") \
        .csv(args.folder_path, sep=',', inferSchema=True, header=True) \
        .withColumn("evaluation",
                    regexp_replace(regexp_extract(input_file_name(), '([^\/]+$)', 0), '.csv|rew_|flatten_|-eval', ''))

    df = df.select(df['duration'], df['total'].alias('tasks'), df['job-parallelism'], df['data-set'], df['evaluation']) \
        .filter(df['state'] == 'FINISHED') \
        .groupBy('job-parallelism', 'data-set', 'evaluation') \
        .agg(round(avg('duration'), 0).alias('duration'), round(avg('tasks'), 0).alias('tasks')) \
        .orderBy('data-set', 'job-parallelism', 'evaluation')

    df.coalesce(1).write.csv(csv_file_path, header='true')


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

