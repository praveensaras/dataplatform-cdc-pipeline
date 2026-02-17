CREATE OR REPLACE PROCEDURE `peoplestrong-in-prod-cloud.etl_job_run.PS_SP_ETL_MERGE_JOB`(bq_target_dataset STRING, bq_target_table STRING, bq_cdc_source_table STRING)
BEGIN
  -- Variables for ETL_JOB_LOG
  DECLARE status_message STRING;
  DECLARE error_message STRING;
  DECLARE run_status INT64;
  DECLARE run_id INT64;
  DECLARE records_inserted INT64;
  DECLARE records_updated INT64;
  DECLARE records_deleted INT64;
  -- DECLARE min_partition_date DATE; -- DD added, for merge op optimization
  -- DECLARE max_partition_date DATE; -- DD added, for merge op optimization

  --DECLARE records_updated_row_key_num INT64; --used to know the row-key-num-conversion
  DECLARE etl_start_time TIMESTAMP;
  DECLARE etl_end_time TIMESTAMP;
  
  -- Variables for ETL_JOB_LOG_INCREMENTAL_DATE
  DECLARE cdc_id INT64;
  DECLARE cdc_start_ts TIMESTAMP;
  DECLARE cdc_end_ts TIMESTAMP;
  DECLARE insert_timestamp TIMESTAMP;
  DECLARE cdc STRING;
  DECLARE insert_flag BOOL;
  DECLARE db STRING;

--changes required for dataform

  DECLARE job_log_upsert_query STRING;
  DECLARE etl_date_table_name STRING;
  DECLARE log_upsert_query STRING;
  DECLARE etl_log_table_name STRING;
  DECLARE source_prefix STRING;
  DECLARE is_active INT64; -- NEW FLAG VARIABLE
  
  -- Config variables
  DECLARE cdc_table STRING;
  DECLARE source_fp STRING;
  DECLARE target_db STRING;
  DECLARE target_fp STRING;
  DECLARE pk STRING;
  DECLARE region STRING;
  DECLARE bq_projectid STRING;
  DECLARE bq_partition_field STRING;
  DECLARE bq_clustering_field STRING;
  DECLARE epoc_cols STRING;
  DECLARE epoc_nano_cols STRING;
  DECLARE epoc_day_cols STRING;
  DECLARE bit_to_int_col STRING;
  DECLARE epoch_to_date_to_string_col STRING;
  DECLARE non_epoch_datetime_col STRING;
  DECLARE datetime_to_int_val_col STRING; ---- '2025-09-07T12:07:01Z' -> 20250907120701
  DECLARE row_key_binary STRING; -- for forming rowkeynum from rowkey
  DECLARE row_key_timestamp STRING; -- for forming rowkeynum from timestamp
  -- declare pk_data_type string;


  
  -- Additional variables
  DECLARE join_key STRING;
  DECLARE cols STRING;
  DECLARE colname STRING;
  DECLARE view_cols STRING;
  DECLARE start_time TIMESTAMP;
  DECLARE end_time TIMESTAMP;
  
  -- Dynamic clauses
  DECLARE partition_by_str STRING;
  DECLARE join_on_str STRING; -- for log_v_i
  DECLARE merge_on_str STRING;
  DECLARE delete_join_on_str STRING; --for log_v_d
  
  SET etl_start_time = CURRENT_TIMESTAMP();
  SET insert_flag = false;

  BEGIN
    -- Get config values (UPDATED: Added row_key_binary)
    -- EXECUTE IMMEDIATE FORMAT("""
    --   SELECT cdc_table,source_fp,target_db,target_fp,pk,region,bq_projectid,bq_target_dataset,bq_partition_field,bq_clustering_field, epoc_cols, epoc_nano_cols, epoc_day_cols, bit_to_int_col, epoch_to_date_to_string_col, non_epoch_datetime_col, datetime_to_int_val_col, row_key_binary, row_key_timestamp
    --   FROM `peoplestrong-in-prod-cloud.etl_job_run.config_file5` a
    --   WHERE a.bq_target_table = '%s' and a.bq_target_dataset = '%s' and a.cdc_table = '%s' """,bq_target_table,bq_target_dataset,bq_cdc_source_table)
    -- INTO cdc_table,source_fp,target_db,target_fp,pk,region,bq_projectid,bq_target_dataset,bq_partition_field,bq_clustering_field, epoc_cols, epoc_nano_cols, epoc_day_cols, bit_to_int_col, epoch_to_date_to_string_col, non_epoch_datetime_col, datetime_to_int_val_col, row_key_binary, row_key_timestamp;

      EXECUTE IMMEDIATE FORMAT("""
      SELECT cdc_table,source_fp,target_db,target_fp,pk,region,bq_projectid,bq_target_dataset,bq_partition_field,bq_clustering_field, epoc_cols, epoc_nano_cols, epoc_day_cols, bit_to_int_col, epoch_to_date_to_string_col, non_epoch_datetime_col, datetime_to_int_val_col,row_key_binary, row_key_timestamp,is_active
      FROM `peoplestrong-in-prod-cloud.etl_job_run.config_file5` a
      WHERE a.bq_target_table = '%s' and a.bq_target_dataset = '%s' and a.cdc_table = '%s' """,bq_target_table,bq_target_dataset,bq_cdc_source_table)
    INTO cdc_table,source_fp,target_db,target_fp,pk,region,bq_projectid,bq_target_dataset,bq_partition_field,bq_clustering_field, epoc_cols, epoc_nano_cols, epoc_day_cols, bit_to_int_col, epoch_to_date_to_string_col, non_epoch_datetime_col, datetime_to_int_val_col ,row_key_binary, row_key_timestamp,is_active;

    SET is_active = COALESCE(is_active,0);

    IF COALESCE(is_active, 0) != 1 THEN
      RETURN; -- Exit procedure early
    END IF;
    
    SET epoc_cols = NULLIF(epoc_cols, '');
    SET epoc_nano_cols = NULLIF(epoc_nano_cols, '');
    SET epoc_day_cols = NULLIF(epoc_day_cols, '');
    SET bit_to_int_col = NULLIF(bit_to_int_col, '');
    SET epoch_to_date_to_string_col = NULLIF(epoch_to_date_to_string_col, '');
    SET non_epoch_datetime_col = NULLIF(non_epoch_datetime_col, '');
    SET datetime_to_int_val_col = NULLIF(datetime_to_int_val_col, '');
    SET row_key_binary = NULLIF(row_key_binary, ''); -- NEW
    SET row_key_timestamp = NULLIF(row_key_timestamp, '');
    -- SET region = (SELECT 'region-' || LOWER(region));
    -- SET join_key = (SELECT REPLACE(pk, ',', '||'));
    -- SET join_key = UPPER(join_key);
    SET cdc = cdc_table;

    -- Get run_id and start_time in a single query
    EXECUTE IMMEDIATE FORMAT("""
      SELECT 
        IFNULL((SELECT MAX(id) FROM `peoplestrong-in-prod-cloud.etl_job_run.ETL_JOB_LOG_INCREMENTAL_DATE`), 0) + 1,
        COALESCE((SELECT MAX(CDC_END_TS) FROM `peoplestrong-in-prod-cloud.etl_job_run.ETL_JOB_LOG_INCREMENTAL_DATE` WHERE CDC_TABLE_NAME = '%s' AND bq_target_dataset1 = '%s'), TIMESTAMP('1970-01-01 00:00:00'))
    """, cdc, bq_target_dataset) INTO run_id, start_time;

    -- set start_time= timestamp('2025-11-10 00:00:00');
    -- SET run_id = IFNULL((SELECT MAX(run_id) FROM `peoplestrong-in-prod-cloud.etl_job_run.ETL_JOB_LOG`), 0) + 1;
    
    SET cdc_id = run_id;

    -- Build dynamic clauses from pk string
    -- SET partition_by_str = (
    --   SELECT STRING_AGG(FORMAT("JSON_VALUE(data.`%s`)", TRIM(col)), ', ')
    --   FROM UNNEST(SPLIT(pk, ',')) AS col
    -- );

-- EXECUTE IMMEDIATE FORMAT("""
--     SELECT STRING_AGG(
--       FORMAT('CAST(JSON_VALUE(d.data.%%s) AS %%s) = i.%%s', 
--         p.col_name,
--         t.data_type,
--         p.col_name
--       ), 
--       ' AND '
--     )
--      FROM (SELECT TRIM(col) AS col_name FROM UNNEST(SPLIT('%s', ',')) AS col) p
--   JOIN (
--     SELECT column_name, data_type 
--     FROM `peoplestrong-in-prod-cloud.%s`.INFORMATION_SCHEMA.COLUMNS 
--     WHERE table_name = '%s'
--   ) t ON p.col_name = t.column_name
-- """, pk, bq_target_dataset, bq_target_table) INTO join_on_str;

-- EXECUTE IMMEDIATE FORMAT("""
--   SELECT STRING_AGG(
--     FORMAT('target.%%s = CAST(JSON_VALUE(source.data.%%s) AS %%s)', 
--       p.col_name,
--       p.col_name,
--       t.data_type
--     ), 
--     ' AND '
--   )
--   FROM (SELECT TRIM(col) AS col_name FROM UNNEST(SPLIT('%s', ',')) AS col) p
--   JOIN (
--     SELECT column_name, data_type 
--     FROM `peoplestrong-in-prod-cloud.%s`.INFORMATION_SCHEMA.COLUMNS 
--     WHERE table_name = '%s'
--   ) t ON p.col_name = t.column_name
-- """, pk, bq_target_dataset, bq_target_table) INTO delete_join_on_str;

-- select delete_join_on_str;
-- select join_on_str;

  EXECUTE IMMEDIATE FORMAT("""
    WITH pk_columns AS (
      SELECT TRIM(col) AS col_name 
      FROM UNNEST(SPLIT('%s', ',')) AS col
    ),
    column_metadata AS (
      SELECT 
        c.column_name, 
        c.data_type,
        CASE WHEN p.col_name IS NOT NULL THEN 1 ELSE 0 END as is_pk_column
      FROM `peoplestrong-in-prod-cloud.%s`.INFORMATION_SCHEMA.COLUMNS c
      LEFT JOIN pk_columns p ON c.column_name = p.col_name
      WHERE c.table_name = '%s'
    )
    SELECT 
      -- All columns for view_cols
      STRING_AGG(FORMAT('`%%s`', column_name), ',') as view_cols,
      -- PK columns only for join conditions
      STRING_AGG(
        CASE WHEN is_pk_column = 1 
          THEN FORMAT('CAST(JSON_VALUE(d.data.`%%s`) AS %%s) = i.%%s', column_name, data_type, column_name)
          ELSE NULL 
        END, 
        ' AND '
      ) as join_on_str,
      STRING_AGG(
        CASE WHEN is_pk_column = 1 
          THEN FORMAT('target.%%s = CAST(JSON_VALUE(source.data.%%s) AS %%s)', column_name, column_name, data_type)
          ELSE NULL 
        END, 
        ' AND '
      ) as delete_join_on_str,

      -- New: partition_by_str from pk
    (SELECT STRING_AGG(FORMAT('JSON_VALUE(data.`%%s`)', col_name), ', ')
     FROM pk_columns) AS partition_by_str,

    -- New: merge_on_str from pk
    (SELECT STRING_AGG(FORMAT('target.%%s = source.%%s', col_name, col_name), ' AND ')
     FROM pk_columns) AS merge_on_str

    FROM column_metadata
  """, pk, bq_target_dataset, bq_target_table) INTO view_cols, join_on_str, delete_join_on_str, partition_by_str, merge_on_str;



    -- SET merge_on_str = (
    --   SELECT STRING_AGG(FORMAT('target.%s = source.%s', TRIM(col), TRIM(col)), ' AND ')
    --   FROM UNNEST(SPLIT(pk, ',')) AS col
    -- );


    SET end_time = CURRENT_TIMESTAMP();

    -- Build column projection string dynamically from INFORMATION_SCHEMA
    EXECUTE IMMEDIATE FORMAT("""
      SELECT STRING_AGG(
        CASE
          -- Condition 0: special handling for SysEndTime
          WHEN column_name IN ('SysEndTime')
          THEN FORMAT("DATETIME('9999-12-31 23:59:59.999999') AS `%%s`", column_name)

          -- Condition 0.5a: Handle row_key_binary conversion
          WHEN '%s' != '' AND LOWER(column_name) = 'rowkeynum'
          THEN FORMAT(
            "CAST((SELECT (code_points[OFFSET(0)] << 56) + (code_points[OFFSET(1)] << 48) + (code_points[OFFSET(2)] << 40) + (code_points[OFFSET(3)] << 32) + (code_points[OFFSET(4)] << 24) + (code_points[OFFSET(5)] << 16) + (code_points[OFFSET(6)] << 8) + code_points[OFFSET(7)] FROM (SELECT TO_CODE_POINTS(FROM_BASE64(JSON_VALUE(data, '$.%%s'))) AS code_points)) AS %%s) AS `%%s`",
            '%s',
            data_type,
            column_name
          )

          -- Condition 0.5b: Handle row_key_timestamp conversion
          WHEN '%s' != '' AND LOWER(column_name) = 'rowkeynum'
          THEN FORMAT(
            "CAST(UNIX_SECONDS(TIMESTAMP(JSON_EXTRACT_SCALAR(data, '$.%%s'))) AS %%s) AS `%%s`",
            '%s',
            data_type,
            column_name
          )

          -- Condition 1: Handle epoch nanosecond columns
          WHEN column_name IN (SELECT TRIM(val) FROM UNNEST(SPLIT(COALESCE('%s', ''), ',')) AS val)
          THEN FORMAT("CAST(TIMESTAMP_MICROS(DIV(CAST(JSON_VALUE(data, '$.%%s') AS INT64), 1000)) AS %%s) AS `%%s`", column_name, data_type, column_name)

          -- Condition 2: Handle epoch day columns
          WHEN column_name IN (SELECT TRIM(val) FROM UNNEST(SPLIT(COALESCE('%s', ''), ',')) AS val)
          THEN FORMAT("CAST(DATE_ADD(DATE '1970-01-01', INTERVAL SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.%%s') AS INT64) DAY) AS %%s) AS `%%s`", column_name, data_type, column_name)

          -- Condition 4: Handle bit to int type conversion
          WHEN column_name IN (SELECT TRIM(val) FROM UNNEST(SPLIT(COALESCE('%s', ''), ',')) AS val)
          THEN FORMAT("CASE JSON_EXTRACT_SCALAR(data, '$.%%s') WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE NULL END AS `%%s`", column_name, column_name)

          -- Condition 5: Handle non epoch datetime col from source
          WHEN column_name IN (SELECT TRIM(val) FROM UNNEST(SPLIT(COALESCE('%s', ''), ',')) AS val)
          THEN FORMAT("CAST(  CAST( JSON_EXTRACT_SCALAR(data, '$.%%s') AS TIMESTAMP) AS  %%s  ) AS `%%s`", column_name, data_type, column_name)

          -- Condition 6: for rowkeytime for some cases, '2025-09-07T12:07:01Z' -> 20250907120701
          WHEN column_name IN (SELECT TRIM(val) FROM UNNEST(SPLIT(COALESCE('%s', ''), ',')) AS val)
          THEN FORMAT(" CAST( FORMAT_TIMESTAMP( '%%%%Y%%%%m%%%%d%%%%H%%%%M%%%%S', CAST( JSON_EXTRACT_SCALAR(data, '$.%%s') AS TIMESTAMP)  ) AS %%s) AS `%%s` ", column_name, data_type, column_name)

          -- Condition 7: Handle standard BOOL columns
          WHEN data_type = 'BOOL'
          THEN FORMAT(
            "CASE WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.%%s')) IN ('true', '1') THEN TRUE WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.%%s')) IN ('false', '0') THEN FALSE ELSE NULL END AS `%%s`", 
            column_name, column_name, column_name)

          -- Condition 8a: Handle TIMESTAMP columns (ISO format strings like "2021-02-11T17:02:30Z") --praveen added to remove utc from the result
          WHEN data_type = 'TIMESTAMP'
          THEN FORMAT("CAST(CAST(JSON_EXTRACT_SCALAR(data, '$.%%s') AS TIMESTAMP) AS DATETIME) AS `%%s`", column_name, column_name)  

          -- Condition 8b: Handle standard DATETIME columns
          WHEN data_type = 'DATETIME'
          THEN FORMAT("DATETIME(TIMESTAMP_MILLIS(CAST(CAST(JSON_EXTRACT_SCALAR(data, '$.%%s') AS NUMERIC) AS INT64))) AS `%%s`", column_name, column_name)

          -- Condition 9: Handle JSON data type columns
          WHEN data_type = 'JSON'
          THEN FORMAT("safe.parse_json(json_value(data, '$.%%s')) AS %%s", column_name, column_name)

          -- Default case for all other data types
          ELSE
            FORMAT("CAST(JSON_EXTRACT_SCALAR(data, '$.%%s') AS %%s) AS `%%s`", column_name, data_type, column_name)
        END,
        ','
      )
      FROM `peoplestrong-in-prod-cloud.%s`.INFORMATION_SCHEMA.COLUMNS
      WHERE table_name = '%s'
        AND LOWER(column_name) NOT IN (
          'message_id', 'source_ts', 'publish_time', 'bq_load_ts', 'source_db_table', 'subscription_name', 'pos',
          'bigquery_updated_on', 'source_ts_ns_order'
        )
    """, 
    COALESCE(row_key_binary, ''), COALESCE(row_key_binary, ''),
    COALESCE(row_key_timestamp, ''), COALESCE(row_key_timestamp, ''),
    COALESCE(epoc_nano_cols, ''),
    COALESCE(epoc_day_cols, ''),
    COALESCE(bit_to_int_col, ''),
    COALESCE(non_epoch_datetime_col, ''),
    COALESCE(datetime_to_int_val_col, ''),
    bq_target_dataset, bq_target_table
    ) INTO cols;

    -- Create insert view using bq_load_ts for windowing
    EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE log_v_i AS
    WITH RankedRecords AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY JSON_VALUE(data.__ts_ns) DESC, JSON_VALUE(data.__source_pos) DESC) as rn
      FROM `%s`
      WHERE JSON_VALUE(data.__op) IS NOT NULL
        AND bq_load_ts > ('%t')
        AND bq_load_ts <= ('%t')
        AND JSON_VALUE(data.__op) != 'd'
    )
    SELECT
      %s,
      TIMESTAMP_MICROS(CAST(CAST(JSON_VALUE(data.__ts_ns) AS INT64) / 1000 AS INT64)) AS source_ts_ns_order,
      publish_time,
      bq_load_ts,
      CAST(JSON_VALUE(data.__source_pos) AS INT64) AS pos,
      current_timestamp as BIGQUERY_UPDATED_ON
    FROM RankedRecords
    WHERE rn = 1
    ORDER BY source_ts_ns_order DESC, pos DESC""", partition_by_str, source_fp, start_time, end_time, cols);

    -- change! -- done
    -- EXECUTE IMMEDIATE "SELECT MAX(bq_load_ts), MIN(bq_load_ts), COUNT(*) FROM log_v_i" INTO cdc_end_ts, cdc_start_ts, records_inserted;
    -- change! -- done
    -- EXECUTE IMMEDIATE "SELECT MIN(bq_load_ts) FROM log_v_i" INTO cdc_start_ts;
   

    -- Create delete view
    EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE log_v_d AS
    WITH RankedRecords AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY JSON_VALUE(data.__ts_ns) DESC, JSON_VALUE(data.__source_pos) DESC) as rn
      FROM `%s`
      WHERE JSON_VALUE(data.__op) IS NOT NULL
        AND bq_load_ts > ('%t')
        AND bq_load_ts <= ('%t')
        AND JSON_VALUE(data.__op) = 'd'
    )
    SELECT d.*
    FROM RankedRecords d
    LEFT JOIN log_v_i i ON %s
    WHERE d.rn = 1 AND (i.%s IS NULL OR i.source_ts_ns_order < TIMESTAMP_MICROS(CAST(CAST(JSON_VALUE(d.data.__ts_ns) AS INT64) / 1000 AS INT64)))
    ORDER BY TIMESTAMP_MICROS(CAST(CAST(JSON_VALUE(d.data.__ts_ns) AS INT64) / 1000 AS INT64)) DESC, CAST(JSON_VALUE(d.data.__source_pos) AS INT64) DESC""", partition_by_str, source_fp, start_time, end_time, join_on_str, (SELECT TRIM(SPLIT(pk, ',')[OFFSET(0)])));

    -- SET (min_partition_date, max_partition_date) = (
    --   SELECT
    --     STRUCT(MIN(DATE(CreationDate)), MAX(DATE(CreationDate)))
    --   FROM
    --     log_v_i
    -- );

    -- Merge operations
    
    EXECUTE IMMEDIATE """
      SELECT 
        (SELECT MAX(bq_load_ts) FROM log_v_i) as max_ts,
        (SELECT MIN(bq_load_ts) FROM log_v_i) as min_ts,
        (SELECT COUNT(*) FROM log_v_i) as insert_count,
        (SELECT COUNT(*) FROM log_v_d) as delete_count
    """ INTO cdc_end_ts, cdc_start_ts, records_inserted, records_deleted;
    
    BEGIN
      BEGIN TRANSACTION;

      -- Get target table columns
--      EXECUTE IMMEDIATE FORMAT("""
--   SELECT STRING_AGG(FORMAT('`%%s`', column_name), ',')
--   FROM `peoplestrong-in-prod-cloud.%s`.INFORMATION_SCHEMA.COLUMNS
--   WHERE table_name = '%s'
-- """, bq_target_dataset, bq_target_table) INTO view_cols;

      SET insert_timestamp = CURRENT_TIMESTAMP();

      -- SET records_updated_row_key_num = 0;  --to know conversion done sucessfully or not  -- DD removed, not needed
      -- IF row_key_binary IS NOT NULL OR row_key_timestamp IS NOT NULL THEN
      --   EXECUTE IMMEDIATE FORMAT("""
      --     SELECT COUNT(*) 
      --     FROM log_v_i 
      --     WHERE Rowkeynum IS NULL
      --   """) INTO records_updated_row_key_num;
      -- END IF; -- DD removed, not needed
      
      -- Get counts before merge for calculating inserts/updates
      -- change! -- done
      -- EXECUTE IMMEDIATE FORMAT("SELECT COUNT(*) FROM log_v_i") INTO records_inserted; dd edited, removed records_updated count
      -- change! -- done
      -- EXECUTE IMMEDIATE FORMAT("SELECT COUNT(*) FROM `%s` WHERE %s", target_fp, 
      --   (SELECT STRING_AGG(FORMAT('%s IN (SELECT %s FROM log_v_i)', TRIM(col), TRIM(col)), ' AND ')
      --    FROM UNNEST(SPLIT(pk, ',')) AS col)
      -- ) INTO records_updated; -- dd edited, removed records_updated count
      
        -- AND DATE(TARGET.CreationDate) >= '%t'
        -- AND DATE(TARGET.CreationDate) <= '%t'
        -- min_partition_date,
        -- max_partition_date,

      EXECUTE IMMEDIATE FORMAT("""
        MERGE INTO `%s` AS target
        USING (SELECT %s FROM log_v_i) AS source
        ON %s
        WHEN MATCHED THEN UPDATE SET %s
        WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
      """,
        target_fp,
        view_cols,
        merge_on_str,
        (SELECT STRING_AGG(FORMAT("target.%s = source.%s", col, col))  -- ✅ REPLACE 
 FROM UNNEST(SPLIT(view_cols, ',')) AS col),
        view_cols,
        (SELECT STRING_AGG(FORMAT("source.%s", col))  -- ✅ REPLACE 
FROM UNNEST(SPLIT(view_cols, ',')) AS col)
      );

      -- Calculate actual inserts (total - updates)
      -- SET records_inserted = records_inserted - records_updated; -- dd edited, removed records_updated count


      -- Delete merge (only if there are delete records)
      -- change! -- done
      -- EXECUTE IMMEDIATE "SELECT COUNT(*) FROM log_v_d" INTO records_deleted; -- reuse variable
      -- change! -- done
      IF records_deleted > 0 THEN
        EXECUTE IMMEDIATE FORMAT("""
          MERGE INTO `%s` AS target
          USING log_v_d AS source
          ON %s
          WHEN MATCHED THEN DELETE
        """, target_fp, delete_join_on_str  -- Using the pre-calculated, type-safe join string
        );
      END IF;
      COMMIT TRANSACTION;
      SET run_status = 1;
      SET etl_end_time = CURRENT_TIMESTAMP();
      SET insert_flag = true;
    EXCEPTION WHEN ERROR THEN
      SET insert_flag = false;
      SET run_status = 0;
      SET error_message = @@error.message;
      SET etl_end_time = CURRENT_TIMESTAMP(); 

      -- change! -- done
      --SELECT error_message AS DEBUG_ERROR;
      ROLLBACK TRANSACTION;

      -- change! -- done
      -- INSERT INTO `peoplestrong-in-prod-cloud.etl_job_run.ETL_JOB_LOG` 
      -- (RUN_ID, PROC_NAME, TARGET_TABLE, START_TIME, RUN_STATUS, ERROR_MSG, RECORDS_INSERTED, RECORDS_UPDATED, END_TIME, row_key_num_conversion)
      -- VALUES (run_id, 'SP_ETL_MERGE_JOB', bq_target_table, etl_start_time, 'FAILED', error_message, 0, 0, etl_end_time , 'ERROR_OCCURRED');
  
      RAISE USING MESSAGE = error_message; 
    END;
  END;

  set source_prefix = REPLACE(SPLIT(cdc_table, '.')[OFFSET(0)], '_cdc', ''); -- change for dataform dml issue handle


  SET status_message = CASE
    WHEN run_status = 0 THEN 'FAILED'
    WHEN run_status = 1 THEN 'SUCCESS'
    ELSE 'PENDING'
  END;

  --new etl job log table change for dml changes 

  -- SET
  -- etl_log_table_name = CONCAT('peoplestrong-in-prod-cloud.temp.ETL_JOB_LOG_', source_prefix,'_',bq_target_table);

  -- -- Upsert into ETL_JOB_LOG_<prefix>_<table>
  -- SET
  --   log_upsert_query = FORMAT("""
  -- MERGE `%s` T
  -- USING (
  --   SELECT
  --     %d AS run_id,
  --     'SP_ETL_MERGE_JOB' AS proc_name,
  --     '%s' AS target_table,
  --     TIMESTAMP('%s') AS start_time,
  --     TIMESTAMP('%s') AS end_time,
  --     '%s' AS run_status,
  --     '%s' AS error_msg,
  --     %d AS records_inserted,
  --     %d AS records_updated
  -- ) S
  -- ON TRUE
  -- WHEN MATCHED THEN UPDATE SET
  --   run_id = S.run_id,
  --   proc_name = S.proc_name,
  --   target_table = S.target_table,
  --   start_time = S.start_time,
  --   end_time = S.end_time,
  --   run_status = S.run_status,
  --   error_msg = S.error_msg,
  --   records_inserted = S.records_inserted,
  --   records_updated = S.records_updated
  -- WHEN NOT MATCHED THEN INSERT (
  --   run_id, proc_name, target_table, start_time, end_time, run_status, error_msg, records_inserted, records_updated
  -- ) VALUES (
  --   S.run_id, S.proc_name, S.target_table, S.start_time, S.end_time, S.run_status, S.error_msg, S.records_inserted, S.records_updated
  -- );
  -- """,
  -- etl_log_table_name,
  -- run_id,
  -- COALESCE(bq_target_table,''),
  -- COALESCE(CAST(etl_start_time AS STRING),''),
  -- COALESCE(CAST(etl_end_time AS STRING),CAST(CURRENT_TIMESTAMP() AS STRING)),
  -- COALESCE(status_message,''),
  -- COALESCE(error_message,''),
  -- COALESCE(records_inserted,0),
  -- COALESCE(records_updated,0)
  -- );

  -- EXECUTE IMMEDIATE log_upsert_query;

  SET
    etl_date_table_name = CONCAT('peoplestrong-in-prod-cloud.temp.ETL_JOB_LOG_INCREMENTAL_DATE_', source_prefix,'_',bq_target_table);



-- INSERT INTO `peoplestrong-in-prod-cloud.etl_job_run.ETL_JOB_LOG_test` (RUN_ID, PROC_NAME, TARGET_TABLE, START_TIME, RUN_STATUS, ERROR_MSG, RECORDS_INSERTED, RECORDS_UPDATED, END_TIME, row_key_num_conversion) 
-- VALUES (run_id, 'SP_ETL_MERGE_JOB', bq_target_table, etl_start_time, status_message, error_message, records_inserted, records_updated, etl_end_time, 
-- CASE WHEN row_key_binary IS NULL AND row_key_timestamp IS NULL THEN 'NOT_APPLICABLE' WHEN records_updated_row_key_num > 0 THEN 'CONVERSION_FAILED' ELSE 'CONVERSION_SUCCESS' END);


  -- Upsert into ETL_JOB_LOG_INCREMENTAL_DATE_<prefix>_<table>
  SET
    job_log_upsert_query = FORMAT("""
  MERGE `%s` T
  USING (
    SELECT
      %d AS ID,
      '%s' AS CDC_TABLE_NAME,
      '%s' AS bq_target_dataset1,
      TIMESTAMP('%s') AS CDC_START_TS,
      TIMESTAMP('%s') AS CDC_END_TS,
      TIMESTAMP('%s') AS INSERT_TIMESTAMP,
      '%s' AS target_table,
      '%s' AS run_status,
      '%s' AS error_msg,
      %d AS records_inserted,
      %d AS records_deleted
  ) S
  ON TRUE
  WHEN MATCHED THEN UPDATE SET
    ID = S.ID,
    CDC_TABLE_NAME = S.CDC_TABLE_NAME,
    bq_target_dataset1 = S.bq_target_dataset1,
    CDC_START_TS = S.CDC_START_TS,
    CDC_END_TS = S.CDC_END_TS,
    INSERT_TIMESTAMP = S.INSERT_TIMESTAMP,
    target_table = S.target_table,
    run_status = S.run_status,
    error_msg = S.error_msg,
    records_inserted = S.records_inserted,
    records_deleted = S.records_deleted
  WHEN NOT MATCHED THEN INSERT (
    ID, CDC_TABLE_NAME, bq_target_dataset1, CDC_START_TS, CDC_END_TS, INSERT_TIMESTAMP, target_table, run_status, error_msg, records_inserted, records_deleted
  ) VALUES (
    S.ID, S.CDC_TABLE_NAME, S.bq_target_dataset1, S.CDC_START_TS, S.CDC_END_TS, S.INSERT_TIMESTAMP, S.target_table, S.run_status, S.error_msg, S.records_inserted, S.records_deleted
  );
  """,
  etl_date_table_name,
  cdc_id,
  COALESCE(cdc,''),
  COALESCE(bq_target_dataset,''),
  COALESCE(CAST(cdc_start_ts AS STRING),'1970-01-01 00:00:00'),
  COALESCE(CAST(cdc_end_ts AS STRING),'1970-01-01 00:00:00'),
  CAST(insert_timestamp AS STRING),
  COALESCE(bq_target_table,''),
  COALESCE(status_message,''),
  COALESCE(error_message,''),
  COALESCE(records_inserted,0),
  COALESCE(records_deleted,0)
  );

  EXECUTE IMMEDIATE job_log_upsert_query;

END;