**Description**

Create csv file to be uploaded to Salesforce with segmentation data.

Segmentation data is filtered. We send to Salesforce's sftp only
data for clients who accepted to be contacted by email. This is for
volume restrictions.

**Source**

1. **Customer Centric Segmentation**:

 * Format: Parquet compressed files
 * Location: `s3://bucket-cdr-main-{environment}/datalake/crm.db/customer_centric_segmentation/partition_date={year}-{month}-{day}`
 * Columns: 519
 * Rows: ~12 millions rows/partition
 * Size: ~2 GB

2. **Client Permisions**:

 * Format: CSV file
 * Location: `s3://bucket-cdr-main-{environment}/CRM/SAS/customer_centric/customers/cli_permisos/cli_permisos.csv`
 * Columns: 1
 * Rows: 6-7 millions
 * Size: 60 MB

*Sample Data*

```csv
1
3
51
122
144
169
237
251
252
260
```

**Destination**

Only 153 columns from `customer_centric_segmentation` are used.

The data is exported to CSV in the following S3 path:

`s3://bucket-cdr-main-{environment}{}/tmp/cust_centric_init`

Partitions are not coalesced so there are multiple files in above directory.

**Execution baseline**

CSV size is approximately 3 GB and execution time about 1.5 minutes.

| Stage Id | Description | Submitted | Duration | Tasks: Succeeded/Total | Input | Output | Shuffle Read | Shuffle Write |
| --- | --- | --- | --- | --- | --- | --- | ---- | ---- |
| 3 | save at NativeMethodAccessorImpl.java:0 | 2019/08/26 14:59:10 | 34 s | 200/200 | | 3.2 GB | 5.1 GB | |
| 2	| save at NativeMethodAccessorImpl.java:0 | 2019/08/26 14:58:53 | 17 s | 36/36 | 1002.4 MB | | | 5.1 MB |
| 1	| save at NativeMethodAccessorImpl.java:0 | 2019/08/26 14:58:53 | 5 s | 15/15 | 59.2 MB | | | 33.5 GB|
| 0	| parquet at NativeMethodAccessorImpl.java:0 | 2019/08/26 14:58:46 | 2 s | 1/1 | | | | |

