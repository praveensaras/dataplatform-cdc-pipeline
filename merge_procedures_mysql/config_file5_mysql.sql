-- MySQL 8.4 / Percona 8.4 equivalent of
-- peoplestrong-in-prod-cloud.etl_job_run.config_file5
--
-- This table drives the generic CDC merge procedure. Each row
-- describes how to read from a raw CDC table and merge into a
-- corresponding silver table.
--
-- NOTE:
-- - Column names are kept close to the BigQuery version for easier
--   cross-reference, but types are mapped to MySQL.
-- - pk is currently assumed to be a single column name; the initial
--   stored procedure implementation uses only the first PK column.
-- - Intentionally omitted (BQ-only, not needed for MySQL): region,
--   bq_projectid, bq_clustering_field, bit_to_int_col,
--   epoch_to_date_to_string_col, row_key_binary, row_key_timestamp,
--   row_key_int, row_key_bigint.

CREATE DATABASE IF NOT EXISTS etl_job_run
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE etl_job_run;

CREATE TABLE IF NOT EXISTS config_file5 (
  cdc_table                       VARCHAR(255)  NOT NULL, -- logical CDC table name, e.g. neo.bot_master_cdc
  source_fp                       VARCHAR(255)  NOT NULL, -- fully-qualified raw CDC table: database.table
  source_database                 VARCHAR(255)  NOT NULL, -- source mysql database 
  target_fp                       VARCHAR(255)  NOT NULL, -- fully-qualified silver table: databse.table_name
  mysql_target_table                 VARCHAR(255)  NOT NULL, -- logical target table name
  pk                              VARCHAR(255)  NOT NULL, -- comma-separated PK column names (JSON keys)
  mysql_target_database          VARCHAR(255)  NOT NULL, -- logical dataset; we map this to target_db
  mysql_partition_field              VARCHAR(255)  NULL,
  epoc_cols                       TEXT          NULL,
  epoc_nano_cols                  TEXT          NULL,
  epoc_day_cols                   TEXT          NULL,
  non_epoch_datetime_col          TEXT          NULL,
  datetime_to_int_val_col         TEXT          NULL,
  datetime_null                   VARCHAR(255)  NULL,
  col_value_as_current_time_ist   VARCHAR(255)  NULL,
  epoc_datetime_ist               VARCHAR(255)  NULL,
  is_active                       TINYINT(1)    NOT NULL DEFAULT 1,

  PRIMARY KEY (mysql_target_database, mysql_target_table, cdc_table)
) ENGINE=InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;

