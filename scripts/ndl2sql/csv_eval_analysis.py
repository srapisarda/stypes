import argparse
import re
from pathlib import Path
from shutil import rmtree
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_replace, regexp_extract, avg, round, max, min


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

    # .join(df_total, ['job-parallelism', 'data-set', 'evaluation'])\

    df = df.select(df['duration'], df['total'].alias('tasks'), df['job-parallelism'], df['data-set'], df['evaluation']) \
        .filter(df['state'] == 'FINISHED') \
        .groupBy('job-parallelism', 'data-set', 'evaluation') \
        .agg(round(avg('duration'), 0).alias('duration'), round(avg('tasks'), 0).alias('tasks')) \
        .orderBy('data-set', 'job-parallelism', 'evaluation')

    df.show()

    df_min_max = df.select(df['duration'], df['tasks'], df['job-parallelism'], df['data-set']) \
        .groupBy('job-parallelism', 'data-set') \
        .agg(max('duration').alias('max-duration'), min('duration').alias('min-duration'),
             max('tasks').alias('max-tasks'), min('tasks').alias('min-tasks')) \
        .orderBy('data-set', 'job-parallelism')
    df_min_max.show()

    df_with_stats = df.join(df_min_max, ['job-parallelism', 'data-set']) \
        .withColumn('duration-min-increase',
                    round((df['duration'] - df_min_max['min-duration']) / df_min_max['min-duration'], 2)) \
        .withColumn('duration-max-decrease',
                    round((df_min_max['max-duration'] - df['duration']) / df_min_max['max-duration'], 2)) \
        .withColumn('tasks-min-increase',
                    round((df['tasks'] - df_min_max['min-tasks']) / df_min_max['min-tasks'], 2)) \
        .withColumn('tasks-max-decrease',
                    round((df_min_max['max-tasks'] - df['tasks']) / df_min_max['max-tasks'], 2)) \
        .select('job-parallelism', 'data-set', 'evaluation', 'duration', 'tasks', 'duration-min-increase', 'tasks-min-increase') \:q!
        .orderBy('data-set', 'job-parallelism')

    df_with_stats.show()

    df_with_stats.coalesce(1).write.csv(csv_file_path, header='true')


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
