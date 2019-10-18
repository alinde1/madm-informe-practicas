**Description**

ETL to get error codes about disponibility with their description in
spanish and english and save them into a HIVE table.


**Source**

1. **ODS**:

 * Format: Oracle Table (ODS)
 * Location: `fuentes.TB_DWC_TMP_SIRCMT02_ELMNTDESC`
 * Columns: 3
 * Rows: 152
 * Size:

*Command to retrieve data*

```sql
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
```

*Sample*

| iblueerror.code | iblueerror.es_description | iblueerror.en_description |
|----|----|----|
|ACT   |          Las fechas de la reserva no cumplen con la norma de activación       |                                   Dates do not match the rate activation rule|
|AG    |          Error de agencia                                                     |                                   Agency error                                |
|AG_1  |          Propiedades de agencia incorrectas                                   |                                   Agency properties error                      |
|AG_2  |          Fiabilidad de agencia no válida                                      |                                   Invalid agency reliability                    |

**Destination**

Save the data into `tracking.iblueerror`.

The data rarely changes. There is a monthly Airflow task to update (overwrite) the table.

**Execution baseline**


