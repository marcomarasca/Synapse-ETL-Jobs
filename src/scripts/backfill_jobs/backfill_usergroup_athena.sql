CREATE EXTERNAL TABLE backfill.old_usergroup ( 
    change_timestamp bigint, 
    record_type string, 
    json_record string
) 
PARTITIONED BY (instance string, snapshot_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( "separatorChar"=",")
LOCATION 's3://prod.snapshot.record.sagebase.org/'
TBLPROPERTIES ( 
    "storage.location.template"="s3://prod.snapshot.record.sagebase.org/${instance}/usergroup/${snapshot_date}/", 
    "projection.enabled"="true", 
    "projection.instance.type"="integer", 
    -- Stack 386 is the first stack that has data for 2022, stack 452 is the last stack without the new stream
    "projection.instance.range"="000000386,000000452", 
    "projection.instance.digits"="9", 
    "projection.snapshot_date.type"="date", 
    "projection.snapshot_date.format"="yyyy-MM-dd", 
    -- We backfill from Jan 2022 up until the 21st of May 2023 which is the last day of stack 452
    "projection.snapshot_date.range"="2022-01-01,2023-05-21"
);

CREATE EXTERNAL TABLE backfill.transformed_usergroup(
  `stack` string, 
  `instance` string, 
  `objecttype` string, 
  `changetype` string, 
  `changetimestamp` bigint, 
  `snapshottimestamp` bigint, 
  `userid` bigint, 
  `snapshot` struct<
    id:string,
    isIndividual:boolean,
    etag:string,
    creationDate:bigint,
    uri:string
   >
)
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hive.hcatalog.data.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://prod.log.sagebase.org/userGroupSnapshots/records_backfill'
TBLPROPERTIES ( 
  'write.compression'='GZIP'
);

-- This is the backfill query, notice that an insert is limited to 100 partitions so we cannot run this on the entire dataset but we can filter by instance (e.g. 5 instances at the time)
INSERT INTO backfill.transformed_acl_snapshots
SELECT 
'prod' as stack,
-- Gets rid of the zero padding
cast(cast(instance as integer) as varchar) as instance,
'PRINCIPAL' as objectType,
-- We do not have the changeType in the old data, if the timstamp of the change is more than a second after the creation date treat it as an UPDATE
IF (date_diff('millisecond', from_unixtime(change_timestamp/1000), from_iso8601_timestamp(json_extract_scalar(json_record, '$.creationDate'))) > 1000, 'UPDATE', 'CREATE') as changeType,
change_timestamp as changeTimestamp,
cast(to_unixtime(CAST(snapshot_date AS TIMESTAMP)) * 1000 as bigint) as snapshotTimestamp,
-- We do not have the user id that made the change in the old data
null as userId,
CAST(ROW(
    json_extract_scalar(json_record, '$.id'), 
    json_extract_scalar(json_record, '$.isIndividual'), 
    json_extract_scalar(json_record, '$.etag'),
-- The old dataware house stores timestamps as iso8601 rather than bigints
    cast(to_unixtime(from_iso8601_timestamp(json_extract_scalar(json_record, '$.creationDate'))) * 1000 as bigint), 
    json_extract_scalar(json_record, '$.uri')
) AS ROW(
    id varchar, 
    isIndividual boolean, 
    etag varchar, 
    creationDate bigint, 
    uri varchar
)) 
AS snapshot,
cast(year(date(snapshot_date)) as varchar) as year,
-- Zero pads month and day
lpad(cast(month(date(snapshot_date)) as varchar), 2, '0') as month,
lpad(cast(day(date(snapshot_date)) as varchar), 2, '0') as day
FROM old_acl_snapshots where instance IN 
('000000386', '000000387', '000000388', '000000389', '000000390', '000000391', '000000392', '000000393', '000000394', '000000395');
('000000396', '000000397', '000000398', '000000399', '000000400', '000000401', '000000402', '000000403', '000000404', '000000405');
('000000406', '000000407', '000000408', '000000409', '000000410', '000000411', '000000412', '000000413', '000000414', '000000415');
('000000416', '000000417', '000000418', '000000419', '000000420', '000000421', '000000422', '000000423', '000000424', '000000425');
('000000426', '000000427', '000000428', '000000429', '000000430', '000000431', '000000432', '000000433', '000000434', '000000435');
('000000436', '000000437', '000000438', '000000439', '000000440', '000000441', '000000442', '000000443', '000000444', '000000445');
('000000446', '000000447', '000000448', '000000449', '000000450', '000000451', '000000452');

