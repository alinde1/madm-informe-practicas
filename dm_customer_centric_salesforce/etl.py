"""
Send data to Salesforce via sftp
"""

import sys
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, concat, lit


def parse_args():
    """
    Parse arguments
    """
    try:
        start_date = sys.argv[1]
    except IndexError:
        start_date = time.strftime("%Y-%m-%d")

    try:
        env = sys.argv[2]
    except IndexError:
        env = 'devel'

    return start_date, env


def concat_cols(*cols):
    """
    Concatenate columns
    """
    columns = coalesce(cols[0], lit("*"))
    for col in cols[1:]:
        columns = concat(*[columns, lit('|'), coalesce(col, lit("*"))])
    return columns


def read_columns(name):
    """
    Read column names from file
    """
    filename = 'schemas/{}'.format(name)
    with open(filename) as file:
        cols = file.readlines()
    cols = [x.strip().lower() for x in cols]
    return cols


def bucket(env):
    """ Bucket name """
    return 's3://bucket-cdr-main-{}'.format(env)


def segmentation(date, env):
    """ Path to segmentation parquet file """
    parquet_path = 'datalake/crm.db/customer_centric_segmentation'
    return '{}/{}/partition_date={}'.format(bucket(env), parquet_path, date)


def cli_permisos(env):
    """ Path to cli_permisos csv """
    cli_permisos_path = 'CRM/SAS/customer_centric/customers/cli_permisos'
    return '{}/{}/cli_permisos.csv'.format(bucket(env), cli_permisos_path)


def csv_path(env):
    """ Path to export csv """
    return '{}/tmp/cust_centric_init'.format(bucket(env))


def main():
    """
    ETL process
    """

    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    start_date, env = parse_args()

    cols1 = columns1()  # read_columns('columns1.txt')
    cols2 = columns2()  # read_columns('columns2.txt')
    cols3 = columns3()  # read_columns('columns3.txt')
    alias_col2 = "beh_concat_affinity_hotel_1_5"

    logging.info("Create Spark Session")
    spark = (SparkSession.builder
             .master('yarn')
             .appName('customer_centric_salesforce')
             .enableHiveSupport()
             .config('hive.exec.dynamic.partition', 'true')
             .config('hive.exec.dynamic.partition.mode', 'nonstrict')
             .config('spark.driver.memory', '5G')
             .getOrCreate())

    logging.info("Read customer_centric_segmentation")
    df_segmentation = spark.read.parquet(segmentation(start_date, env))
    df_segmentation = df_segmentation.select(cols1 + cols3)
    df_segmentation = df_segmentation.withColumn(alias_col2, concat_cols(*cols2))
    df_segmentation = df_segmentation.select(cols1 + [alias_col2] + cols3)

    logging.info("Read cli_permisos")
    schema = df_segmentation.select('id_nu_cliente').schema
    df_cli_permisos = spark.read.csv(cli_permisos(env), schema=schema)

    logging.info("Joining data frames")
    df1 = df_segmentation.alias('df1')
    df2 = df_cli_permisos.alias('df2')
    df_customer = df1.join(df2, df1.id_nu_cliente == df2.id_nu_cliente).select('df1.*')
    df_customer = df_customer.withColumn('partition_date', lit(start_date))

    logging.info("Write csv files")
    df_customer.write.format("com.databricks.spark.csv")\
                     .option("emptyValue", None)\
                     .option("nullValue", None)\
                     .option("sep", ";")\
                     .option("header", "true")\
                     .save(csv_path(env), mode="overwrite")

    spark.stop()

    logging.info("Done.")


def columns1():
   """Temporal workarround"""
   cols = ['id_nu_cliente',
           'id_nu_cliente_sirius',
           'id_v2_cliente_web',
           'con_campaign_language',
           'rwd_birthday_date',
           'con_flag_contact_allowled',
           'con_geo_zip_code',
           'rwd_balance_points',
           'con_email_bounced_type',
           'con_email_flag_contact',
           'rwd_card_creation_date',
           'rwd_points_expired_date',
           'rwd_point_expiration_in_days',
           'rwd_is_online',
           'rwd_near_upgrade_to_tier',
           'rwd_near_downgrade_to_tier',
           'rwd_active_campaign',
           'con_mail_status',
           'clc_flag_leisure_long_stay',
           'clc_flag_business',
           'clc_flag_leisure_next3m',
           'beh_propensity_affinity_tax_1',
           'beh_propensity_affinity_tax_2',
           'beh_propensity_affinity_tax_3',
           'beh_propensity_affinity_tax_4',
           'beh_propensity_affinity_tax_5',
           'beh_propensity_affinity_tax_6',
           'beh_propensity_affinity_tax_7',
           'beh_propensity_affinity_tax_8',
           'beh_propensity_affinity_tax_9',
           'beh_propensity_affinity_tax_10',
           'beh_propensity_affinity_tax_11',
           'beh_propensity_affinity_tax_12',
           'beh_propensity_affinity_tax_13',
           'beh_propensity_affinity_tax_14',
           'beh_propensity_affinity_tax_15',
           'beh_propensity_affinity_tax_16',
           'beh_propensity_affinity_tax_17',
           'beh_propensity_affinity_tax_18',
           'beh_propensity_affinity_tax_19',
           'beh_propensity_affinity_tax_20',
           'beh_propensity_affinity_tax_21',
           'beh_propensity_affinity_tax_22',
           'beh_propensity_affinity_tax_23',
           'beh_propensity_affinity_tax_24',
           'beh_propensity_affinity_tax_25',
           'beh_propensity_affinity_tax_26',
           'beh_propensity_affinity_tax_27',
           'beh_propensity_affinity_tax_28',
           'dem_number_of_children',
           'dem_town',
           'con_contact_flag_phone',
           'con_contact_flag_mobile',
           'dem_age',
           'dem_sex',
           'dem_contact_subscription_date',
           'dem_contact_unsubscribe_date',
           'clc_ltv',
           'rwd_customer_type',
           'rwd_cutomer_tier',
           'beh_searches_last_month',
           'beh_searches_last_quarter',
           'beh_searches_last_week',
           'beh_anticipation_last_month',
           'beh_anticipation_last_quarter',
           'beh_anticipation_last_week',
           'beh_searches_l12m',
           'beh_search_last_hotel',
           'beh_search_last_brand',
           'beh_search_last_step',
           'beh_search_freq_hotel_l12m',
           'clc_stays_l12m',
           'clc_stays_lm',
           'clc_nights_l12m',
           'clc_nights_lm',
           'clc_rooms_l12m',
           'clc_rooms_lm',
           'clc_revenue_l12m',
           'clc_revenue_lm',
           'clc_last_stay_hotel',
           'clc_last_stay_brand',
           'clc_last_stay_fnb',
           'clc_last_stay_room_type',
           'clc_last_stay_channel_type',
           'beh_preferred_hotel_hist',
           'beh_preferred_hotel_l12m',
           'beh_preferred_brand_hist',
           'beh_preferred_brand_l12m',
           'beh_preferred_stay_type_hist',
           'beh_preferred_stay_type_l12m',
           'beh_preferred_hotel_type_hist',
           'beh_preferred_hotel_type_l12m',
           'rwd_point_earned_hist',
           'rwd_point_redem_hist',
           'clc_stays_cancelled_lm',
           'clc_stays_cancelled_l12m',
           'clc_actual_clc',
           'clc_previous_clc',
           'beh_preferred_hotel_1',
           'beh_preferred_hotel_2',
           'beh_preferred_hotel_3',
           'clc_class_arr_los',
           'clc_class_arr_business',
           'clc_class_arr_leisure_short',
           'clc_class_arr_leisure_long',
           'clc_value_nro_stays_leisure_l12m',
           'clc_value_nro_stays_leisure_hist',
           'clc_value_nro_stays_busin_l12m',
           'clc_value_nro_stays_busin_hist',
           'clc_value_nro_stays_l12m',
           'clc_value_nro_stays_hist',
           'beh_segment_business',
           'beh_antpt_leisure_longstay',
           'dem_contact_mail',
           'dem_geo_state',
           'rwd_point_balance',
           'dem_country',
           'con_contact_flag_email',
           'rwd_stakeholder',
           'con_balance_saturation',
           'beh_affinity_hotel_1',
           'beh_affinity_hotel_2',
           'beh_affinity_hotel_3',
           'beh_affinity_hotel_4',
           'beh_affinity_hotel_5']
   return cols


def columns2():
    """Temporal workarround"""
    cols = ['beh_affinity_hotel_2',
            'beh_affinity_hotel_3',
            'beh_affinity_hotel_4',
            'beh_affinity_hotel_5']
    return cols


def columns3():
    """Temporal workarround"""
    cols = ['con_saturation_capacity',
            'beh_propensity_top_rooms',
            'dem_age_child_1',
            'dem_age_child_2',
            'beh_propensity_arrival_month_0',
            'beh_propensity_arrival_month_1',
            'beh_propensity_arrival_month_2',
            'beh_propensity_arrival_month_3',
            'beh_propensity_arrival_month_4',
            'beh_propensity_arrival_month_5',
            'beh_propensity_arrival_month_6',
            'beh_propensity_arrival_month_7',
            'beh_propensity_arrival_month_8',
            'beh_propensity_arrival_month_9',
            'beh_propensity_arrival_month_10',
            'beh_propensity_arrival_month_11']
    return cols


if __name__ == "__main__":
    main()
