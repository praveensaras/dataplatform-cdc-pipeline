CREATE TABLE `peoplestrong-in-prod-cloud.etl_job_run.ETL_JOB_LOG_INCREMENTAL_DATE`
(
  ID INT64,
  CDC_TABLE_NAME STRING,
  bq_target_dataset1 STRING,
  CDC_START_TS TIMESTAMP,
  CDC_END_TS TIMESTAMP,
  INSERT_TIMESTAMP TIMESTAMP,
  target_table STRING,
  error_msg STRING,
  run_status STRING,
  records_inserted INT64,
  records_deleted INT64
);