-- This is the table that points to the old data warehouse snapshots
CREATE EXTERNAL TABLE backfill.old_acl_snapshots (
    change_timestamp bigint,
    record_type string,
    json_record string
) PARTITIONED BY (instance string, snapshot_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (
   "separatorChar"=","
)
LOCATION 's3://prod.snapshot.record.sagebase.org/'
TBLPROPERTIES (
    "storage.location.template"="s3://prod.snapshot.record.sagebase.org/${instance}/aclrecord/${snapshot_date}/",
    "projection.enabled"="true",
    "projection.instance.type"="integer",
-- The stack 386 is the first stack that spans 2022
    "projection.instance.range"="000000386,000000457",
    "projection.instance.digits"="9",
    "projection.snapshot_date.type"="date",
    "projection.snapshot_date.format"="yyyy-MM-dd",
-- Only backfill from Jan 2022
    "projection.snapshot_date.range"="2022-01-01,2023-06-19"
);

-- This is the table that represents the destination and serializes in JSON format
CREATE EXTERNAL TABLE backfill.transformed_acl_snapshots(
  `stack` string, 
  `instance` string, 
  `objecttype` string, 
  `changetype` string, 
  `changetimestamp` bigint, 
  `snapshottimestamp` bigint, 
  `userid` bigint, 
  `snapshot` struct<
    id:string,
    createdby:string,
    creationdate:bigint,
    modifiedby:string,
    modifiedon:bigint,
    etag:string,
    resourceaccess:array<
        struct<
            principalid:int,
            accesstype:array<string>
        >
    >,
    ownertype:string>
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
-- Temporary location for the backfilled data, we can then copy the backfilled records to the real destination
-- s3://prod.log.sagebase.org/aclSnapshots/records
  's3://prod.log.sagebase.org/aclSnapshots/records_backfill'
TBLPROPERTIES ( 
  'write.compression'='GZIP'
);

-- This is the backfill query, notice that an insert is limited to 100 partitions so we cannot run this on the entire dataset but we can filter by instance (e.g. 5 instances at the time)
INSERT INTO backfill.transformed_acl_snapshots
SELECT 
'prod' as stack,
-- Gets rid of the zero padding
cast(cast(instance as integer) as varchar) as instance,
'ACCESS_CONTROL_LIST' as objectType,
-- We do not have the changeType in the old data, if the timstamp of the change is more than a second after the creation date treat it as an UPDATE
IF (date_diff('millisecond', from_unixtime(change_timestamp/1000), from_iso8601_timestamp(json_extract_scalar(json_record, '$.creationDate'))) > 1000, 'UPDATE', 'CREATE') as changeType,
change_timestamp as changeTimestamp,
cast(to_unixtime(CAST(snapshot_date AS TIMESTAMP)) * 1000 as bigint) as snapshotTimestamp,
cast(json_extract_scalar(json_record, '$.createdBy') as bigint) as userId,
CAST(ROW(
    json_extract_scalar(json_record, '$.id'), 
    json_extract_scalar(json_record, '$.createdBy'), 
-- The old dataware house stores timestamps as iso8601 rather than bigints
    cast(to_unixtime(from_iso8601_timestamp(json_extract_scalar(json_record, '$.creationDate'))) * 1000 as bigint), 
    json_extract_scalar(json_record, '$.createdBy'), 
    cast(to_unixtime(from_iso8601_timestamp(json_extract_scalar(json_record, '$.modifiedOn'))) * 1000 as bigint), 
    json_extract_scalar(json_record, '$.etag'),
    json_parse(json_query(json_record, 'lax $.resourceAccess')),
    json_extract_scalar(json_record, '$.ownerType')
) AS ROW(
    id varchar, 
    createdBy varchar, 
    creationDate bigint, 
    modifiedBy varchar, 
    modifiedOn bigint, 
    etag varchar, 
    resourceAccess ARRAY(
        ROW(principalId int, accessType ARRAY(varchar))
    ), 
    ownerType varchar)
) 
AS snapshot,
cast(year(date(snapshot_date)) as varchar) as year,
-- Zero pads month and day
lpad(cast(month(date(snapshot_date)) as varchar), 2, '0') as month,
lpad(cast(day(date(snapshot_date)) as varchar), 2, '0') as day
FROM old_acl_snapshots where instance IN ('000000455', '000000456', '000000457');