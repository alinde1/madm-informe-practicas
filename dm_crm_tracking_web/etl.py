"""
Load tracking data to S3
"""

from __future__ import print_function
import argparse
import time
from datetime import datetime, timedelta

from jinja2 import Environment, FileSystemLoader
from pyspark.sql import SparkSession
import logging


def parse_arguments():
    """
    Return start date and environment
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("exec_date", help="Execution date: YYYY-MM-DD[_HH]",
                        nargs='?', default=time.strftime("%Y-%m-%d"))
    parser.add_argument("env", help="Environment: 'devel' or 'live'",
                        nargs='?', default="devel")
    parser.add_argument("end_date", help="Execute from exec_date to end_date",
                        nargs='?', default=None)
    args = parser.parse_args()

    try:
        start_date, _ = args.exec_date.split('_')
    except ValueError:
        start_date = args.exec_date

    if args.end_date:
        end_date = args.end_date
    else:
        end_date = start_date

    return start_date, args.env, end_date


def date_iter(start_date, end_date):
    """
    Iterate between start and end dates
    """
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    delta = (end_dt - start_dt).days
    date_inc = delta and (1, -1)[delta < 0]
    while start_dt != end_dt:
        yield start_dt.strftime("%Y-%m-%d")
        start_dt = (start_dt + timedelta(days=date_inc))
    yield end_dt.strftime("%Y-%m-%d")


def create_temp_table(spark, name, with_hotel=True):
    """Create temporary table"""
    file_loader = FileSystemLoader('templates')
    template = Environment(loader=file_loader).get_template(name+'.sql.j2')
    sql = template.render(with_hotel=with_hotel)
    data_frame = spark.sql(sql)
    data_frame.createOrReplaceTempView("temp_table_"+name)
    return data_frame


def create_traffic_data_frame(spark, with_hotel=True):
    """
    Create temporary table with traffic data
    """
    create_temp_table(spark, 'src_normalized_h', with_hotel)
    create_temp_table(spark, 'base', with_hotel)
    create_temp_table(spark, 'sessions', with_hotel)
    return create_temp_table(spark, 'traffic', with_hotel)


def create_src_raw_h(spark, date, env):
    """
    Create in-memory temporal copy of src_raw_h partition
    """
    bucket = 's3://bucket-cdr-main-{}'.format(env)
    path = 'datalake/tracking.db/tracking_web/src_raw_h/partition_date={}'.format(date)
    src_raw_h_path = '{}/{}/'.format(bucket, path)
    src_raw_h = spark.read.parquet(src_raw_h_path)
    src_raw_h.createOrReplaceTempView('tmp_src_raw_h')


def load_data(data_frame, date, env, with_hotel=True):
    """
    Export data to S3.
    """

    if with_hotel:
        path = '/CRM/tracking/web/trafico_con_hotel/'
    else:
        path = '/CRM/tracking/web/trafico_sin_hotel/'

    s3_path = 's3://bucket-cdr-main-' + env + path + date
    data_frame.write.csv(s3_path, header=True, mode="overwrite")


def main():
    """
    Load data with and without hotel
    """
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    start_date, env, end_date = parse_arguments()

    spark = (SparkSession.builder
             .appName('etl_crm_tracking_web')
             .enableHiveSupport()
             .config("hive.exec.dynamic.partition", "true")
             .config("hive.exec.dynamic.partition.mode", "nonstrict")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    for date in date_iter(start_date, end_date):
        logging.info('[{}] Loading data'.format(date))

        logging.info('[{}] Load src_raw_h partition into memory'.format(date))
        create_src_raw_h(spark, date, env)

        logging.info('[{}] Create traffic data with hotel'.format(date))
        df_traffic = create_traffic_data_frame(spark)
        logging.info('[{}] Load traffic data with hotel into S3'.format(date))
        load_data(df_traffic, date, env)

        logging.info('[{}] Create traffic data without hotel'.format(date))
        df_traffic = create_traffic_data_frame(spark, with_hotel=False)
        logging.info('[{}] Load traffic data without hotel into S3'.format(date))
        load_data(df_traffic, date, env, with_hotel=False)

    spark.stop()


if __name__ == "__main__":
    main()
