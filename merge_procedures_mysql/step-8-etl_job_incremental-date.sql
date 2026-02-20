-- Step 6: step-5 + transaction block + BQ-style MERGE (upsert from log_v_i, then delete from log_v_d).
-- Mimics BigQuery merge.sql: MERGE via INSERT ... ON DUPLICATE KEY UPDATE, then DELETE when matched.

DROP PROCEDURE IF EXISTS etl_job_run.sp_cdc_merge_job;
DELIMITER $$

CREATE PROCEDURE etl_job_run.sp_cdc_merge_job (
    IN p_mysql_target_database VARCHAR(255),
    IN p_mysql_target_table    VARCHAR(255),
    IN p_cdc_table             VARCHAR(255)
)
proc_main: BEGIN
    -- ------------------------------------------------------------------
    -- Variables for ETL job status
    -- ------------------------------------------------------------------
    DECLARE v_status_message      VARCHAR(32);
    DECLARE v_error_message       TEXT;
    DECLARE v_run_status          INT;
    DECLARE v_run_id              VARCHAR(36);
    DECLARE v_records_inserted    BIGINT;
    DECLARE v_records_updated     BIGINT;
    DECLARE v_records_deleted     BIGINT;

    DECLARE v_etl_start_time      DATETIME(6);
    DECLARE v_etl_end_time        DATETIME(6);

    DECLARE v_cdc_id              VARCHAR(36);
    DECLARE v_cdc_start_ts        DATETIME(6);
    DECLARE v_cdc_end_ts          DATETIME(6);
    DECLARE v_insert_timestamp    DATETIME(6);
    DECLARE v_cdc                 VARCHAR(255);
    DECLARE v_insert_flag         TINYINT(1);
    DECLARE v_db                  VARCHAR(255);

    -- ------------------------------------------------------------------
    -- Config variables (from etl_job_run.config_file5)
    -- ------------------------------------------------------------------
    DECLARE v_cdc_table                   VARCHAR(255);
    DECLARE v_source_fp                   VARCHAR(255);
    DECLARE v_source_db                  VARCHAR(255);
    DECLARE v_target_fp                   VARCHAR(255);
    DECLARE v_mysql_target_table          VARCHAR(255);
    DECLARE v_pk                          VARCHAR(255);
    DECLARE v_mysql_target_database       VARCHAR(255);
    DECLARE v_mysql_partition_field       VARCHAR(255);
    DECLARE v_epoc_cols                   TEXT;
    DECLARE v_epoc_nano_cols              TEXT;
    DECLARE v_epoc_day_cols               TEXT;
    DECLARE v_non_epoch_datetime_col      TEXT;
    DECLARE v_datetime_to_int_val_col     TEXT;
    DECLARE v_datetime_null               VARCHAR(255);
    DECLARE v_col_value_as_current_time_ist VARCHAR(255);
    DECLARE v_epoc_datetime_ist           VARCHAR(255);
    DECLARE v_is_active                   TINYINT(1);

    -- ------------------------------------------------------------------
    -- Additional variables (dynamic SQL, windowing)
    -- ------------------------------------------------------------------
    DECLARE v_join_key            VARCHAR(255);
    DECLARE v_cols                LONGTEXT;
    DECLARE v_colname             VARCHAR(255);
    DECLARE v_view_cols           TEXT;
    DECLARE v_start_time          DATETIME(6);
    DECLARE v_end_time            DATETIME(6);
    DECLARE v_target_schema       VARCHAR(255);
    DECLARE v_target_table_name   VARCHAR(255);
    DECLARE v_pk_first_col        VARCHAR(255);
    DECLARE v_source_schema       VARCHAR(255);
    DECLARE v_source_table        VARCHAR(255);

    DECLARE v_partition_by_str    TEXT;
    DECLARE v_join_on_str         TEXT;
    DECLARE v_merge_on_str        TEXT;
    DECLARE v_delete_join_on_str  TEXT;
    DECLARE v_target_i_join_str   TEXT;
    DECLARE v_t_i_join_str        TEXT;
    DECLARE v_i_pk_null_check_str TEXT;
    DECLARE v_update_set_str      TEXT;

    DECLARE v_sql                 TEXT;

    -- Handler: on any SQL error in transaction block, rollback and re-raise
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SET v_run_status   = 0;
        SET v_insert_flag  = 0;
        SET v_etl_end_time = NOW(6);
        GET DIAGNOSTICS CONDITION 1 v_error_message = MESSAGE_TEXT;
        RESIGNAL;
    END;

    -- ------------------------------------------------------------------
    -- Initial setup
    -- ------------------------------------------------------------------
    SET v_etl_start_time = NOW(6);
    SET v_insert_flag    = 0;
    SET SESSION group_concat_max_len = 4294967295;

    -- ------------------------------------------------------------------
    -- 1) Load config
    -- ------------------------------------------------------------------
    SELECT
        cdc_table,
        source_fp,
        source_db,
        target_fp,
        mysql_target_table,
        pk,
        mysql_target_database,
        mysql_partition_field,
        epoc_cols,
        epoc_nano_cols,
        epoc_day_cols,
        non_epoch_datetime_col,
        datetime_to_int_val_col,
        datetime_null,
        col_value_as_current_time_ist,
        epoc_datetime_ist,
        is_active
    INTO
        v_cdc_table,
        v_source_fp,
        v_source_db,
        v_target_fp,
        v_mysql_target_table,
        v_pk,
        v_mysql_target_database,
        v_mysql_partition_field,
        v_epoc_cols,
        v_epoc_nano_cols,
        v_epoc_day_cols,
        v_non_epoch_datetime_col,
        v_datetime_to_int_val_col,
        v_datetime_null,
        v_col_value_as_current_time_ist,
        v_epoc_datetime_ist,
        v_is_active
    FROM etl_job_run.config_file5
    WHERE mysql_target_table    = p_mysql_target_table
      AND mysql_target_database = p_mysql_target_database
      AND cdc_table             = p_cdc_table
    LIMIT 1;

    SET v_is_active = COALESCE(v_is_active, 0);
    IF v_cdc_table IS NULL OR v_is_active <> 1 THEN
        LEAVE proc_main;
    END IF;

    -- ------------------------------------------------------------------
    -- 2) Normalize config string fields
    -- ------------------------------------------------------------------
    SET v_epoc_cols               = NULLIF(v_epoc_cols, '');
    SET v_epoc_nano_cols          = NULLIF(v_epoc_nano_cols, '');
    SET v_epoc_day_cols           = NULLIF(v_epoc_day_cols, '');
    SET v_non_epoch_datetime_col  = NULLIF(v_non_epoch_datetime_col, '');
    SET v_datetime_to_int_val_col = NULLIF(v_datetime_to_int_val_col, '');
    SET v_datetime_null           = NULLIF(v_datetime_null, '');
    SET v_col_value_as_current_time_ist = NULLIF(v_col_value_as_current_time_ist, '');
    SET v_epoc_datetime_ist       = NULLIF(v_epoc_datetime_ist, '');

    SET v_cdc = v_cdc_table;

    -- ------------------------------------------------------------------
    -- 3) Get previous CDC window start_time; generate UUID for run_id (concurrency-safe)
    -- ------------------------------------------------------------------
    SELECT
        COALESCE(MAX(cdc_end_ts), TIMESTAMP('1970-01-01 00:00:00'))
    INTO
        v_start_time
    FROM etl_job_run.etl_job_log_incremental_date
    WHERE mysql_target_table    = v_mysql_target_table
      AND mysql_target_database = v_mysql_target_database;

    SET v_run_id = UUID();
    SET v_cdc_id = v_run_id;

    -- ------------------------------------------------------------------
    -- 4) Build helper strings: view_cols, PK-based partition/join strings (multi-PK aware)
    -- ------------------------------------------------------------------
    SET v_target_schema     = SUBSTRING_INDEX(v_target_fp, '.', 1);
    SET v_target_table_name = SUBSTRING_INDEX(v_target_fp, '.', -1);

    DROP TEMPORARY TABLE IF EXISTS _tmp_view_cols;
    CREATE TEMPORARY TABLE _tmp_view_cols (view_cols TEXT);

    SET @stmt_sql = CONCAT(
        'INSERT INTO _tmp_view_cols SELECT ',
        'GROUP_CONCAT(CONCAT(''`'', column_name, ''`'') ORDER BY ordinal_position SEPARATOR '','') ',
        'FROM INFORMATION_SCHEMA.COLUMNS ',
        'WHERE TABLE_SCHEMA = ''', v_target_schema, ''' AND TABLE_NAME = ''', v_target_table_name, ''''
    );

    PREPARE stmt_view_cols FROM @stmt_sql;
    EXECUTE stmt_view_cols;
    DEALLOCATE PREPARE stmt_view_cols;

    SELECT view_cols INTO v_view_cols FROM _tmp_view_cols LIMIT 1;
    DROP TEMPORARY TABLE _tmp_view_cols;

    -- Split v_pk (comma-separated) into individual columns in _pk_cols
    DROP TEMPORARY TABLE IF EXISTS _pk_cols;
    CREATE TEMPORARY TABLE _pk_cols (col_name VARCHAR(255));

    SET v_join_key = v_pk;

    pk_loop: LOOP
        SET v_colname = TRIM(SUBSTRING_INDEX(v_join_key, ',', 1));

        IF v_colname IS NULL OR v_colname = '' THEN
            LEAVE pk_loop;
        END IF;

        INSERT INTO _pk_cols (col_name) VALUES (v_colname);

        IF INSTR(v_join_key, ',') = 0 THEN
            LEAVE pk_loop;
        END IF;

        SET v_join_key = SUBSTRING(v_join_key, INSTR(v_join_key, ',') + 1);
    END LOOP pk_loop;

    -- Build multi-PK aware helper strings
    SELECT GROUP_CONCAT(
             CONCAT('JSON_UNQUOTE(JSON_EXTRACT(data, ''$.\"', col_name, '\"''))')
             SEPARATOR ', '
           )
    INTO v_partition_by_str
    FROM _pk_cols;

    SELECT GROUP_CONCAT(
             CONCAT('JSON_UNQUOTE(JSON_EXTRACT(d.data, ''$.\"', col_name, '\"'')) = i.', col_name)
             SEPARATOR ' AND '
           )
    INTO v_join_on_str
    FROM _pk_cols;

    SELECT GROUP_CONCAT(
             CONCAT('target.', col_name,
                    ' = JSON_UNQUOTE(JSON_EXTRACT(source.data, ''$.\"', col_name, '\"''))')
             SEPARATOR ' AND '
           )
    INTO v_delete_join_on_str
    FROM _pk_cols;

    SELECT GROUP_CONCAT(
             CONCAT('target.', col_name, ' = i.', col_name)
             SEPARATOR ' AND '
           )
    INTO v_target_i_join_str
    FROM _pk_cols;

    SELECT GROUP_CONCAT(
             CONCAT('t.', col_name, ' = i.', col_name)
             SEPARATOR ' AND '
           )
    INTO v_t_i_join_str
    FROM _pk_cols;

    SELECT GROUP_CONCAT(
             CONCAT('target.', col_name, ' = source.', col_name)
             SEPARATOR ' AND '
           )
    INTO v_merge_on_str
    FROM _pk_cols;

    SELECT GROUP_CONCAT(
             CONCAT('i.', col_name, ' IS NULL')
             SEPARATOR ' AND '
           )
    INTO v_i_pk_null_check_str
    FROM _pk_cols;

    -- Debug: show PK parsing and helper strings
    SELECT v_pk AS pk_raw;
    SELECT col_name FROM _pk_cols;
    SELECT
        v_view_cols          AS view_cols,
        v_partition_by_str   AS partition_by_str,
        v_join_on_str        AS join_on_str,
        v_delete_join_on_str AS delete_join_on_str,
        v_target_i_join_str  AS target_i_join_str,
        v_t_i_join_str       AS t_i_join_str,
        v_merge_on_str       AS merge_on_str;

    -- ------------------------------------------------------------------
    -- 5) Build v_cols: per-column expression from JSON -> typed (dynamic)
    -- ------------------------------------------------------------------
    SET v_cols = NULL;

    SET @epoc_nano_list   = COALESCE(TRIM(REPLACE(REPLACE(v_epoc_nano_cols, ' ', ''), CHAR(10), '')), '');
    SET @epoc_day_list    = COALESCE(TRIM(REPLACE(REPLACE(v_epoc_day_cols, ' ', ''), CHAR(10), '')), '');
    SET @epoc_list        = COALESCE(TRIM(REPLACE(REPLACE(v_epoc_cols, ' ', ''), CHAR(10), '')), '');
    SET @non_epoch_list   = COALESCE(TRIM(REPLACE(REPLACE(v_non_epoch_datetime_col, ' ', ''), CHAR(10), '')), '');
    SET @current_ist_list = COALESCE(TRIM(REPLACE(REPLACE(v_col_value_as_current_time_ist, ' ', ''), CHAR(10), '')), '');

    DROP TEMPORARY TABLE IF EXISTS _tmp_cols;
    CREATE TEMPORARY TABLE _tmp_cols (c LONGTEXT);

    SET @stmt_sql = CONCAT(
        'INSERT INTO _tmp_cols ',
        'SELECT GROUP_CONCAT(',
        '  CASE ',
        '    WHEN FIND_IN_SET(column_name, @epoc_nano_list) > 0 THEN CONCAT(''CAST(FROM_UNIXTIME(CAST(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(l.data, ''''$.\"'', column_name, ''\"'''')) , ''''null'''') AS UNSIGNED)/1000000000) AS '', COLUMN_TYPE, '') AS `'', column_name, ''`'') ',
        '    WHEN FIND_IN_SET(column_name, @epoc_day_list) > 0 THEN CONCAT(''CAST(DATE_ADD(''''1970-01-01'''', INTERVAL CAST(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(l.data, ''''$.\"'', column_name, ''\"'''')) , ''''null'''') AS SIGNED) DAY) AS '', COLUMN_TYPE, '') AS `'', column_name, ''`'') ',
        '    WHEN FIND_IN_SET(column_name, @epoc_list) > 0 THEN CONCAT(''CAST(FROM_UNIXTIME(CAST(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(l.data, ''''$.\"'', column_name, ''\"'''')) , ''''null'''') AS UNSIGNED)) AS '', COLUMN_TYPE, '') AS `'', column_name, ''`'') ',
        '    WHEN FIND_IN_SET(column_name, @non_epoch_list) > 0 THEN CONCAT(''CAST(REPLACE(REPLACE(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(l.data, ''''$.\"'', column_name, ''\"'''')), ''''null''''), ''''T'''', '''' ''''), ''''Z'''', '''''''') AS DATETIME) AS `'', column_name, ''`'') ',
        '    WHEN FIND_IN_SET(column_name, @current_ist_list) > 0 THEN CONCAT(''CAST(CONVERT_TZ(NOW(), @@session.time_zone, ''''+05:30'''') AS '', COLUMN_TYPE, '') AS `'', column_name, ''`'') ',
        '    WHEN column_name = ''PLACE'' THEN ''CAST(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(l.data, ''''$.\"place\"'''')), ''''null'''') AS CHAR(255)) AS `PLACE`'' ',
        '    ELSE CONCAT(''CAST(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(l.data, ''''$.\"'', column_name, ''\"'''')) , ''''null'''') AS '', ',
        '      CASE DATA_TYPE WHEN ''int'' THEN ''SIGNED'' WHEN ''bigint'' THEN ''SIGNED'' WHEN ''tinyint'' THEN ''SIGNED'' WHEN ''smallint'' THEN ''SIGNED'' WHEN ''mediumint'' THEN ''SIGNED'' ',
        '        WHEN ''datetime'' THEN ''DATETIME'' WHEN ''timestamp'' THEN CONCAT(''DATETIME('', COALESCE(DATETIME_PRECISION, 0), '')'') WHEN ''date'' THEN ''DATE'' WHEN ''time'' THEN ''TIME'' ',
        '        WHEN ''varchar'' THEN CONCAT(''CHAR('', COALESCE(CHARACTER_MAXIMUM_LENGTH, 255), '')'') WHEN ''char'' THEN CONCAT(''CHAR('', COALESCE(CHARACTER_MAXIMUM_LENGTH, 255), '')'') ',
        '        WHEN ''decimal'' THEN CONCAT(''DECIMAL('', COALESCE(NUMERIC_PRECISION, 10), '', '', COALESCE(NUMERIC_SCALE, 0), '')'') ',
        '        WHEN ''double'' THEN ''DOUBLE'' WHEN ''float'' THEN ''DOUBLE'' WHEN ''json'' THEN ''JSON'' ELSE ''CHAR(255)'' END, '') AS `'', column_name, ''`'') ',
        '  END ',
        '  ORDER BY ORDINAL_POSITION SEPARATOR '','') ',
        'FROM INFORMATION_SCHEMA.COLUMNS ',
        'WHERE TABLE_SCHEMA = ''', REPLACE(v_target_schema, "'", "''"), ''' AND TABLE_NAME = ''', REPLACE(v_target_table_name, "'", "''"), ''''
    );

    PREPARE stmt_cols FROM @stmt_sql;
    EXECUTE stmt_cols;
    DEALLOCATE PREPARE stmt_cols;

    SELECT c INTO v_cols FROM _tmp_cols LIMIT 1;
    DROP TEMPORARY TABLE _tmp_cols;

    SELECT v_cols AS cols_built;

    -- Length check: LENGTH() = bytes, CHAR_LENGTH() = characters. TEXT max = 65535 bytes.
    SELECT
        LENGTH(v_cols)        AS v_cols_bytes,
        CHAR_LENGTH(v_cols)   AS v_cols_chars,
        65535                 AS text_limit_bytes,
        IF(LENGTH(v_cols) > 65535, 'TRUNCATED if stored in TEXT', 'OK') AS v_cols_note;

    -- ------------------------------------------------------------------
    -- 6) Build log_v_i = target shape (typed cols + metadata). Flow:
    --    STEP 1: Filter by time window (mysql_load_ts > v_start_time AND <= v_end_time)
    --    STEP 2: Exclude deletes (__op != 'd')
    --    STEP 3–4: Partition by PK, ROW_NUMBER() ORDER BY __ts_ns, __source_pos DESC
    --    STEP 5: Keep only rn = 1
    --    STEP 6: Transform JSON to columns (v_cols) -> log_v_i looks like target table
    -- ------------------------------------------------------------------
    SET v_end_time = NOW(6);


    SET v_source_schema = SUBSTRING_INDEX(v_source_fp, '.', 1);
    SET v_source_table  = SUBSTRING_INDEX(v_source_fp, '.', -1);

    DROP TEMPORARY TABLE IF EXISTS log_v_i;

    -- Build full SQL via temp table + GROUP_CONCAT (avoids CONCAT with v_cols breaking on quotes)
    DROP TEMPORARY TABLE IF EXISTS _sql_parts;
    CREATE TEMPORARY TABLE _sql_parts (part_no INT PRIMARY KEY, part_val LONGTEXT);

    INSERT INTO _sql_parts (part_no, part_val) VALUES (
        1,
        'CREATE TEMPORARY TABLE log_v_i AS SELECT '
    );
    INSERT INTO _sql_parts (part_no, part_val) VALUES (2, v_cols);
    INSERT INTO _sql_parts (part_no, part_val) VALUES (
        3,
        CONCAT(
            ', l.source_ts_ns_order, l.pos, l.mysql_load_ts, NOW(6) AS mysql_updated_on, l.__op FROM ( ',
            '  WITH ranked AS ( ',
            '    SELECT d.*, ',
            '      ROW_NUMBER() OVER ( ',
            '        PARTITION BY ', v_partition_by_str, ' ',
            '        ORDER BY ',
            '          FROM_UNIXTIME(CAST(JSON_UNQUOTE(JSON_EXTRACT(d.data, ''$.\"__ts_ns\"'')) AS UNSIGNED) / 1000000000) DESC, ',
            '          CAST(JSON_UNQUOTE(JSON_EXTRACT(d.data, ''$.\"__source_pos\"'')) AS SIGNED) DESC ',
            '      ) AS rn ',
            '    FROM `', REPLACE(v_source_schema, '`', '``'), '`.`', REPLACE(v_source_table, '`', '``'), '` AS d ',
            '    WHERE JSON_EXTRACT(d.data, ''$.\"__op\"'') IS NOT NULL ',
            '      AND d.mysql_load_ts > ''', DATE_FORMAT(v_start_time, '%Y-%m-%d %H:%i:%s.%f'), ''' ',
            '      AND d.mysql_load_ts <= ''', DATE_FORMAT(v_end_time, '%Y-%m-%d %H:%i:%s.%f'), ''' ',
            '      AND JSON_UNQUOTE(JSON_EXTRACT(d.data, ''$.\"__op\"'')) <> ''d'' ',
            '  ) ',
            '  SELECT data, ',
            '    FROM_UNIXTIME(CAST(JSON_UNQUOTE(JSON_EXTRACT(data, ''$.\"__ts_ns\"'')) AS UNSIGNED) / 1000000000) AS source_ts_ns_order, ',
            '    CAST(JSON_UNQUOTE(JSON_EXTRACT(data, ''$.\"__source_pos\"'')) AS SIGNED) AS pos, ',
            '    JSON_UNQUOTE(JSON_EXTRACT(data, ''$.\"__op\"'')) AS __op, ',
            '    mysql_load_ts ',
            '  FROM ranked WHERE rn = 1 ',
            ') AS l'
        )
    );

    SELECT GROUP_CONCAT(part_val ORDER BY part_no SEPARATOR '') INTO @stmt_sql FROM _sql_parts;
    DROP TEMPORARY TABLE IF EXISTS _sql_parts;

    -- Length of full SQL that will be passed to PREPARE (max_allowed_packet limits this)
    SELECT
        LENGTH(@stmt_sql)     AS full_sql_bytes,
        CHAR_LENGTH(@stmt_sql) AS full_sql_chars;

    PREPARE stmt_log_i FROM @stmt_sql;
    EXECUTE stmt_log_i;
    DEALLOCATE PREPARE stmt_log_i;

    -- ------------------------------------------------------------------
    -- 7) Show log_v_i result (already target-shaped: typed cols + source_ts_ns_order, pos, mysql_updated_on)
    -- ------------------------------------------------------------------
    SET @stmt_sql = 'SELECT * FROM log_v_i';

    PREPARE stmt_show_i FROM @stmt_sql;
    EXECUTE stmt_show_i;
    DEALLOCATE PREPARE stmt_show_i;

    -- ------------------------------------------------------------------
-- 8) Build log_v_d = latest delete per PK, only if newer than any insert
-- ------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS log_v_d;

    SET @stmt_sql = CONCAT(
    'CREATE TEMPORARY TABLE log_v_d AS ',
    'WITH RankedDeletes AS ( ',
    '  SELECT d.data, ',
    '    ROW_NUMBER() OVER ( ',
    '      PARTITION BY ', v_partition_by_str, ' ',
    '      ORDER BY ',
    '        FROM_UNIXTIME(CAST(JSON_UNQUOTE(JSON_EXTRACT(d.data, ''$.\"__ts_ns\"'')) AS UNSIGNED) / 1000000000) DESC, ',
    '        CAST(JSON_UNQUOTE(JSON_EXTRACT(d.data, ''$.\"__source_pos\"'')) AS SIGNED) DESC ',
    '    ) AS rn ',
    '  FROM `', REPLACE(v_source_schema, '`', '``'), '`.`', REPLACE(v_source_table, '`', '``'), '` AS d ',
    '  WHERE JSON_EXTRACT(d.data, ''$.\"__op\"'') IS NOT NULL ',
    '    AND d.mysql_load_ts > ''', DATE_FORMAT(v_start_time, '%Y-%m-%d %H:%i:%s.%f'), ''' ',
    '    AND d.mysql_load_ts <= ''', DATE_FORMAT(v_end_time, '%Y-%m-%d %H:%i:%s.%f'), ''' ',
    '    AND JSON_UNQUOTE(JSON_EXTRACT(d.data, ''$.\"__op\"'')) = ''d'' ',
    ') ',
    'SELECT d.data, d.rn ',
    'FROM RankedDeletes d ',
    'LEFT JOIN log_v_i i ON ', v_join_on_str, ' ',
    'WHERE d.rn = 1 ',
    '  AND ( ',
    '    (', v_i_pk_null_check_str, ') ',
    '    OR i.source_ts_ns_order < FROM_UNIXTIME(CAST(JSON_UNQUOTE(JSON_EXTRACT(d.data, ''$.\"__ts_ns\"'')) AS UNSIGNED) / 1000000000) ',
    '  ) ',
    'ORDER BY ',
    '  FROM_UNIXTIME(CAST(JSON_UNQUOTE(JSON_EXTRACT(d.data, ''$.\"__ts_ns\"'')) AS UNSIGNED) / 1000000000) DESC, ',
    '  CAST(JSON_UNQUOTE(JSON_EXTRACT(d.data, ''$.\"__source_pos\"'')) AS SIGNED) DESC'
);

    PREPARE stmt_log_d FROM @stmt_sql;
    EXECUTE stmt_log_d;
    DEALLOCATE PREPARE stmt_log_d;

    -- Show result
    SET @stmt_sql = 'SELECT * FROM log_v_d';
    PREPARE stmt_show_d FROM @stmt_sql;
    EXECUTE stmt_show_d;
    DEALLOCATE PREPARE stmt_show_d;

    -- ------------------------------------------------------------------
    -- 9) Counts and CDC window (like BQ: for logging later)
    -- ------------------------------------------------------------------
    SELECT COUNT(*) AS log_v_i_count FROM log_v_i;
    SELECT COUNT(*) AS log_v_d_count FROM log_v_d;

    SET v_cdc_start_ts = v_start_time;
    SELECT MAX(mysql_load_ts) FROM log_v_i INTO v_cdc_end_ts;

    SELECT COUNT(*) FROM log_v_i INTO v_records_inserted;
    SELECT COUNT(*) FROM log_v_d INTO v_records_deleted;

    select v_records_inserted, v_records_deleted;
    select v_start_time, v_end_time;

    -- ------------------------------------------------------------------
    -- 10) Build UPDATE SET clause (target.col = i.col) for op='u' only merge
    -- ------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS _tmp_set;
    CREATE TEMPORARY TABLE _tmp_set (set_str TEXT);
    SET @stmt_sql = CONCAT(
        'INSERT INTO _tmp_set SELECT GROUP_CONCAT(',
        '  CONCAT(''target.`'', column_name, ''` = i.`'', column_name, ''`'') ORDER BY ordinal_position SEPARATOR '', ''',
        ') FROM INFORMATION_SCHEMA.COLUMNS ',
        'WHERE TABLE_SCHEMA = ''', REPLACE(v_target_schema, "'", "''"), ''' AND TABLE_NAME = ''', REPLACE(v_target_table_name, "'", "''"), ''''
    );
    PREPARE stmt_set FROM @stmt_sql;
    EXECUTE stmt_set;
    DEALLOCATE PREPARE stmt_set;
    SELECT set_str INTO v_update_set_str FROM _tmp_set LIMIT 1;
    DROP TEMPORARY TABLE _tmp_set;

    -- ------------------------------------------------------------------
    -- 11) Transaction block: BQ-style MERGE with __op check (update only when op='u')
    -- ------------------------------------------------------------------
    START TRANSACTION;

    -- 11a) UPDATE existing rows only when CDC op = 'u' (avoid overwriting on replay/duplicate)
    SET @stmt_sql = CONCAT(
        'UPDATE `', REPLACE(v_target_schema, '`', '``'), '`.`', REPLACE(v_target_table_name, '`', '``'), '` AS target ',
        'INNER JOIN log_v_i AS i ON ', v_target_i_join_str, ' AND i.`__op` = ''u'' ',
        'SET ', v_update_set_str
    );
    PREPARE stmt_merge FROM @stmt_sql;
    EXECUTE stmt_merge;
    DEALLOCATE PREPARE stmt_merge;

    -- 11b) INSERT only new rows (not already in target); no overwrite on duplicate
    SET @stmt_sql = CONCAT(
        'INSERT INTO `', REPLACE(v_target_schema, '`', '``'), '`.`', REPLACE(v_target_table_name, '`', '``'), '` (', v_view_cols, ') ',
        'SELECT ', v_view_cols, ' FROM log_v_i AS i ',
        'WHERE NOT EXISTS ( ',
        '  SELECT 1 FROM `', REPLACE(v_target_schema, '`', '``'), '`.`', REPLACE(v_target_table_name, '`', '``'), '` AS t ',
        '  WHERE ', v_t_i_join_str, ' ',
        ')'
    );
    PREPARE stmt_ins FROM @stmt_sql;
    EXECUTE stmt_ins;
    DEALLOCATE PREPARE stmt_ins;

    -- 11c) DELETE: remove target rows that are in log_v_d (only if there are deletes)
    IF v_records_deleted > 0 THEN
        SET @stmt_sql = CONCAT(
            'DELETE target FROM `', REPLACE(v_target_schema, '`', '``'), '`.`', REPLACE(v_target_table_name, '`', '``'), '` AS target ',
            'INNER JOIN log_v_d AS source ON ', v_delete_join_on_str, ' '
        );
        PREPARE stmt_del FROM @stmt_sql;
        EXECUTE stmt_del;
        DEALLOCATE PREPARE stmt_del;
    END IF;

    COMMIT;

    SELECT v_run_status, v_records_inserted, v_records_deleted, v_cdc_start_ts, v_cdc_end_ts;

    SET v_run_status   = 1;
    SET v_insert_flag  = 1;
    SET v_etl_end_time = NOW(6);
    SET v_insert_timestamp = v_etl_end_time;

    SET v_status_message = CASE
        WHEN v_run_status = 0 THEN 'FAILED'
        WHEN v_run_status = 1 THEN 'SUCCESS'
        ELSE 'PENDING'
    END;

    INSERT INTO etl_job_run.etl_job_log_incremental_date (
        id,
        cdc_table_name,
        mysql_target_database,
        cdc_start_ts,
        cdc_end_ts,
        insert_timestamp,
        mysql_target_table,
        error_msg,
        run_status,
        records_inserted,
        records_deleted
    ) VALUES (
        v_cdc_id,
        v_cdc,
        v_mysql_target_database,
        v_cdc_start_ts,
        v_cdc_end_ts,
        v_insert_timestamp,
        v_mysql_target_table,
        v_error_message,
        v_status_message,
        v_records_inserted,
        v_records_deleted
    );

    INSERT INTO etl_job_run.etl_job_log (
        run_id,
        proc_name,
        mysql_target_database,
        mysql_target_table,
        start_time,
        end_time,
        run_status,
        error_msg,
        records_inserted,
        records_deleted
    ) VALUES (
        v_run_id,
        'sp_cdc_merge_job',
        v_mysql_target_database,
        v_mysql_target_table,
        v_etl_start_time,
        v_etl_end_time,
        v_status_message,
        v_error_message,
        v_records_inserted,
        v_records_deleted
    )
    ON DUPLICATE KEY UPDATE
        end_time         = VALUES(end_time),
        run_status       = VALUES(run_status),
        error_msg        = VALUES(error_msg),
        records_inserted = VALUES(records_inserted),
        records_deleted  = VALUES(records_deleted);

END proc_main$$

DELIMITER ;
