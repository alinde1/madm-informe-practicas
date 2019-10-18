**Description**

Load AVAYA data from csv files, located in s3, into Hive.

The field `dummy` is used in temporal table because the csv files are terminated with
the delimiter character `|` leaving an additional empty field.

Data is loaded into partitions dynamically using `segstart_utc` field. That's because
the files include data from two different hours and thus neither the `filename` nor the `execution time`
can be used.

**Source**

CSV files located in S3. The files are copied with a bash script from an ftp where Avaya put them.

*S3 bucket*

`s3://bucket-cdr-landing-{environment}/AVAYA/{year}/{month}/{day}/{hour}`

*Sample Data*

```csv
316885|0|0|281|0|281|2019-05-30 16:25:21|2019-05-30 14:25:21|2019-05-30 16:30:02|2019-05-30 14:30:02|0|0|0|0|0||||0|500|0|-1|-1|-1|5|0|0|0|0|0|0|0|0|0|0|0|0|0|0|1|7|0|0|1|0|0|0|0|0|0|0|0|0|0|0|0|10000072371559226321||000000337||11124|||917333400|10048||||||||||||||||||0|0|0|0|0|0|0|
316886|15|0|0|171|246|2019-05-30 16:25:54|2019-05-30 14:25:54|2019-05-30 16:30:15|2019-05-30 14:30:15|75|0|0|128|5||||4116|1116|1330|1016|1116|-1|5|0|0|0|0|0|0|0|0|0|0|0|0|0|1|1|2|5|0|1|0|0|1|0|0|0|0|0|0|0|0|0|10000072611559226354|453611|000000363|443610||18181||443610|441995600016|||||||||453611|||||||||0|1|1|0|0|0|0|
```

**Destination**

Hive table: `avaya.avaya`

