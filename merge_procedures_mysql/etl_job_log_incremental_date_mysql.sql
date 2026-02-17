-- MySQL 8.4 / Percona 8.4 equivalent of
-- peoplestrong-in-prod-cloud.etl_job_run.ETL_JOB_LOG_INCREMENTAL_DATE
--
-- This table tracks the incremental CDC window processed for each
-- (cdc_table, target_dataset, target_table) combination.
--
-- Assumptions:
-- - Database/schema name: etl_job_run
-- - One row per run_id; ID is the logical run identifier.

CREATE DATABASE IF NOT EXISTS etl_job_run
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE etl_job_run;

CREATE TABLE IF NOT EXISTS etl_job_log_incremental_date (
  id                BIGINT          NOT NULL,          -- logical run id
  cdc_table_name    VARCHAR(255)    NOT NULL,
  mysql_target_database VARCHAR(255)   NOT NULL,          -- target dataset / schema identifier
  cdc_start_ts      DATETIME(6)     NULL,
  cdc_end_ts        DATETIME(6)     NULL,
  insert_timestamp  DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  mysql_target_table  VARCHAR(255)  NOT NULL, -- maps to config_file5.mysql_target_table
  error_msg         TEXT            NULL,
  run_status        VARCHAR(32)     NOT NULL,          -- e.g. SUCCESS / FAILED
  records_inserted  BIGINT          NOT NULL DEFAULT 0,
  records_deleted   BIGINT          NOT NULL DEFAULT 0,

  PRIMARY KEY (id, cdc_table_name, mysql_target_database, mysql_target_table),
  KEY idx_cdc_table_database (cdc_table_name, mysql_target_database),
  KEY idx_cdc_time (cdc_table_name, cdc_start_ts, cdc_end_ts)
) ENGINE=InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

