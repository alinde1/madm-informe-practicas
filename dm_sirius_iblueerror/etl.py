import jpype
import jaydebeapi
from pyspark.sql import SparkSession

jHome = jpype.getDefaultJVMPath()
jpype.startJVM(jHome, '-Djava.class.path=/tmp/ojdbc8.jar', '-Duser.timezone="+02:00"')
conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
                          'jdbc:oracle:thin:USER/password@ods-server:1521/SERVICE')
cursor = conn.cursor()

sql = """
SELECT
  S.FK_CMT01_ELE_CDE AS "Código",
  S.DESCRIPTION_TXT AS "Descripción en Español",
  E.DESCRIPTION_TXT AS "Descripción"
FROM
    (
       SELECT
          FK_CMT01_ELE_CDE,
          DESCRIPTION_TXT
        FROM
          fuentes.TB_DWC_TMP_SIRCMT02_ELMNTDESC
        WHERE
          FK_CMT03_TABLE_CDE = 'IBLUEERROR'
          AND CMT_SIRID_CODE = 'S'
        ORDER BY
          FK_CMT01_ELE_CDE
    ) S
INNER JOIN  
    (
        SELECT
          FK_CMT01_ELE_CDE,
          DESCRIPTION_TXT
        FROM
          fuentes.TB_DWC_TMP_SIRCMT02_ELMNTDESC
        WHERE
          FK_CMT03_TABLE_CDE = 'IBLUEERROR'
          AND CMT_SIRID_CODE = 'E'
        ORDER BY
          FK_CMT01_ELE_CDE
    ) E
ON
  S.FK_CMT01_ELE_CDE = E.FK_CMT01_ELE_CDE
"""

spark = (SparkSession.builder
         .master('local')
         .appName('customer_centric_salesforce')
         .enableHiveSupport()
         .config('hive.exec.dynamic.partition', 'true')
         .config('hive.exec.dynamic.partition.mode', 'nonstrict')
         .config('spark.driver.memory', '5G')
         .getOrCreate())

cursor.execute(sql)
table = cursor.fetchall()
df = spark.createDataFrame(table)
df.createOrReplaceTempView('tmp_iblueerror')
spark.sql('INSERT OVERWRITE TABLE tracking.iblueerror SELECT * FROM tmp_iblueerror')
