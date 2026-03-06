CREATE
OR REPLACE PROCEDURE fix_column_types(
    p_schema_name VARCHAR(255),
    p_table_name VARCHAR(255),
    p_extend_percent int
) AS DECLARE v_max_octet_length DECIMAL(65, 10);

v_recommended_type VARCHAR(255);

v_sql TEXT;

v_create_script LONGTEXT;

v_column_list LONGTEXT;

v_first_column BOOLEAN DEFAULT TRUE;

v_pk_columns TEXT;

v_has_pk BOOLEAN DEFAULT FALSE;

v_shard_key TEXT;

v_has_shard_key BOOLEAN DEFAULT FALSE;

v_sort_key TEXT;

v_has_sort_key BOOLEAN DEFAULT FALSE;

v_keys_list TEXT DEFAULT '';

v_select_list TEXT DEFAULT '';

v_text_col_count INT DEFAULT 0;

v_numeric_select_list TEXT DEFAULT '';

v_numeric_col_count INT DEFAULT 0;

v_max_value DECIMAL(65, 10);

v_min_value DECIMAL(65, 10);

v_has_decimals BIGINT;

/*Column Definitions*/
col_query query(
    ordinal_position INT,
    column_name VARCHAR(255),
    data_type VARCHAR(255),
    column_type VARCHAR(255),
    is_nullable VARCHAR(10),
    column_default TEXT,
    extra VARCHAR(255),
    numeric_precision INT,
    numeric_scale INT
) =
SELECT
    ORDINAL_POSITION,
    COLUMN_NAME,
    DATA_TYPE,
    COLUMN_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    EXTRA,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = p_schema_name
    AND TABLE_NAME = p_table_name
ORDER BY
    ORDINAL_POSITION;

text_col_query query(
    column_name VARCHAR(255),
    original_type VARCHAR(255)
) =
SELECT
    column_name,
    original_type
FROM
    temp_column_analysis
WHERE
    UPPER(original_type) IN (
        'VARCHAR',
        'CHAR',
        'TEXT',
        'TINYTEXT',
        'MEDIUMTEXT',
        'LONGTEXT'
    );

numeric_col_query query(
    column_name VARCHAR(255),
    original_type VARCHAR(255),
    numeric_precision INT,
    numeric_scale INT
) =
SELECT
    column_name,
    original_type,
    numeric_precision,
    numeric_scale
FROM
    temp_column_analysis
WHERE
    UPPER(original_type) IN (
        'DECIMAL',
        'FLOAT',
        'DOUBLE',
        'NUMERIC',
        'TINYINT',
        'SMALLINT',
        'INT',
        'INTEGER',
        'MEDIUMINT',
        'BIGINT'
    );

script_query query(
    column_name VARCHAR(255),
    original_type VARCHAR(255),
    original_column_type VARCHAR(255),
    max_octet_length DECIMAL(65, 10),
    min_octet_length DECIMAL(65, 10),
    recommended_type VARCHAR(255),
    is_nullable VARCHAR(10),
    column_default TEXT,
    extra VARCHAR(255),
    numeric_precision INT,
    numeric_scale INT
) =
SELECT
    column_name,
    original_type,
    original_column_type,
    max_octet_length,
    min_octet_length,
    recommended_type,
    is_nullable,
    column_default,
    extra,
    numeric_precision,
    numeric_scale
FROM
    temp_column_analysis
ORDER BY
    ordinal_position;

BEGIN
/*Script Starts*/
v_create_script = CONCAT(
    '-- Optimized DDL for ' || p_schema_name || '.' || p_table_name || '\n'
);

v_create_script = CONCAT(
    v_create_script,
    '-- Create time: ' || NOW() || '\n\n'
);

v_create_script = CONCAT(
    v_create_script,
    'CREATE TABLE IF NOT EXISTS `' || p_schema_name || '`.`' || p_table_name || '_optimized` (\n'
);

v_column_list = '';

/*Get primary key info*/
SELECT
    IFNULL(
        (
            SELECT
                GROUP_CONCAT(
                    COLUMN_NAME
                    ORDER BY
                        ORDINAL_POSITION SEPARATOR ', '
                )
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
                TABLE_SCHEMA = p_schema_name
                AND TABLE_NAME = p_table_name
                AND CONSTRAINT_NAME = 'PRIMARY'
        ),
        NULL
    ) INTO v_pk_columns;

if v_pk_columns IS NOT NULL then v_has_pk = TRUE;

end if;

/*Shard keys*/
SELECT
    IFNULL(
        (
            SELECT
                GROUP_CONCAT(
                    COLUMN_NAME
                    ORDER BY
                        SEQ_IN_INDEX SEPARATOR ', '
                )
            FROM
                INFORMATION_SCHEMA.STATISTICS
            WHERE
                TABLE_SCHEMA = p_schema_name
                AND TABLE_NAME = p_table_name
                AND INDEX_TYPE = 'SHARD'
            LIMIT
                1
        ), NULL
    ) INTO v_shard_key;

if v_shard_key IS NOT NULL
AND TRIM(v_shard_key) != '' then v_has_shard_key = TRUE;

end if;

/*sort keys*/
SELECT
    IFNULL(
        (
            SELECT
                GROUP_CONCAT(
                    COLUMN_NAME
                    ORDER BY
                        SEQ_IN_INDEX SEPARATOR ', '
                )
            FROM
                INFORMATION_SCHEMA.STATISTICS
            WHERE
                TABLE_SCHEMA = p_schema_name
                AND TABLE_NAME = p_table_name
                AND INDEX_TYPE = 'CLUSTERED COLUMNSTORE'
            LIMIT
                1
        ), NULL
    ) INTO v_sort_key;

if v_sort_key IS NOT NULL
AND TRIM(v_sort_key) != '' then v_has_sort_key = TRUE;

end if;

/*temp analyze table*/
DROP TEMPORARY TABLE IF EXISTS temp_column_analysis;

CREATE TEMPORARY TABLE temp_column_analysis (
    ordinal_position INT,
    column_name VARCHAR(255),
    original_type VARCHAR(255),
    original_column_type VARCHAR(255),
    max_octet_length DECIMAL(65, 10),
    min_octet_length DECIMAL(65, 10),
    recommended_type VARCHAR(255),
    is_nullable VARCHAR(10),
    column_default TEXT,
    extra VARCHAR(255),
    numeric_precision INT,
    numeric_scale INT
);

/*check all column rules */
for col_rec in collect(col_query) loop
/*text columns*/
if UPPER(col_rec.data_type) in (
    'VARCHAR',
    'CHAR',
    'TEXT',
    'TINYTEXT',
    'MEDIUMTEXT',
    'LONGTEXT'
) then if v_text_col_count > 0 then v_select_list = CONCAT(v_select_list, ', ');

end if;

v_select_list = CONCAT(
    v_select_list,
    '(CEIL(COALESCE(MAX(OCTET_LENGTH(`' || col_rec.column_name || '`)), 0)*1+100/' || p_extend_percent || ')+5                  -CEIL(COALESCE(MAX(OCTET_LENGTH(`' || col_rec.column_name || '`)), 0)*1+100/' || p_extend_percent || ')%5)                  AS `' || col_rec.column_name || '_len`'
);

v_text_col_count = v_text_col_count + 1;

end if;

/*numeric columns*/
if UPPER(col_rec.data_type) in (
    'DECIMAL',
    'FLOAT',
    'DOUBLE',
    'NUMERIC',
    'TINYINT',
    'SMALLINT',
    'INT',
    'INTEGER',
    'MEDIUMINT',
    'BIGINT'
) then if v_numeric_col_count > 0 then v_numeric_select_list = CONCAT(v_numeric_select_list, ', ');

end if;

v_numeric_select_list = CONCAT(
    v_numeric_select_list,
    'CAST(MAX(`' || col_rec.column_name || '` * 10.00) AS DECIMAL(65,10)) AS `' || col_rec.column_name || '_max`, ',
    'CAST(MIN(`' || col_rec.column_name || '` * 10.00) AS DECIMAL(65,10)) AS `' || col_rec.column_name || '_min`, ',
    'SUM(CASE WHEN (`' || col_rec.column_name || '` * 10.00) != FLOOR(`' || col_rec.column_name || '` * 10.00) THEN 1 ELSE 0 END) AS `' || col_rec.column_name || '_dec`'
);

v_numeric_col_count = v_numeric_col_count + 1;

end if;

/*Insert basic info first*/
INSERT INTO
    temp_column_analysis
VALUES
    (
        col_rec.ordinal_position,
        col_rec.column_name,
        col_rec.data_type,
        col_rec.column_type,
        NULL,
        - - Will be updated NULL,
        - - Will be updated col_rec.data_type,
        - - Will be updated col_rec.is_nullable,
        col_rec.column_default,
        col_rec.extra,
        col_rec.numeric_precision,
        col_rec.numeric_scale
    );

end loop;

/*get all text column info*/
if v_text_col_count > 0 then v_sql = CONCAT(
    'CREATE TEMPORARY TABLE temp_size_results AS SELECT ',
    v_select_list,
    ' FROM `' || p_schema_name || '`.`' || p_table_name || '`'
);

EXECUTE IMMEDIATE v_sql;

/*Now update each text column with its calculated size*/
for update_rec in collect(text_col_query) loop
/*Get the calculated max length from temp table*/
v_sql = CONCAT(
    'SELECT `' || update_rec.column_name || '_len` FROM temp_size_results'
);

EXECUTE IMMEDIATE v_sql INTO v_max_octet_length;

/*Determine recommended type based on max length with actual values*/
if v_max_octet_length = 0 then v_recommended_type = 'VARCHAR(1)';

- - Minimal size for empty columns elseif v_max_octet_length <= 21845 then
/*Use actual size for VARCHAR (SingleStore max is 21845 for VARCHAR)*/
v_recommended_type = CONCAT('VARCHAR(', v_max_octet_length: > bigint, ')');

elseif v_max_octet_length <= 65535 then
/*TEXT can hold up to 64KB*/
v_recommended_type = 'TEXT';

elseif v_max_octet_length <= 16777215 then
/*MEDIUMTEXT can hold up to 16MB*/
v_recommended_type = 'MEDIUMTEXT';

else
/*LONGTEXT for larger*/
v_recommended_type = 'LONGTEXT';

end if;

/*Update temp_column_analysis with calculated values*/
UPDATE
    temp_column_analysis
SET
    max_octet_length = v_max_octet_length,
    recommended_type = v_recommended_type
WHERE
    column_name = update_rec.column_name;

end loop;

/*Cleanup size results table*/
DROP TEMPORARY TABLE IF EXISTS temp_size_results;

end if;

/*Execute single SELECT to get all numeric column stats (multiplied by 10)*/
if v_numeric_col_count > 0 then v_sql = CONCAT(
    'CREATE TEMPORARY TABLE temp_numeric_results AS SELECT ',
    v_numeric_select_list,
    ' FROM `' || p_schema_name || '`.`' || p_table_name || '`'
);

EXECUTE IMMEDIATE v_sql;

/*Now update each numeric column with integer type if no decimals after *10*/
for numeric_rec in collect(numeric_col_query) loop
/*Get the calculated max, min and decimal count (for value * 10)*/
v_sql = CONCAT(
    'SELECT `' || numeric_rec.column_name || '_max`, `' || numeric_rec.column_name || '_min`, `' || numeric_rec.column_name || '_dec` FROM temp_numeric_results'
);

EXECUTE IMMEDIATE v_sql INTO v_max_value,
v_min_value,
v_has_decimals;

/*If no decimal values exist after *10, recommend integer type based on range*/
if v_has_decimals = 0 then
/*Determine smallest signed integer type that can hold the range*/
if v_min_value >= -128
AND v_max_value <= 127 then v_recommended_type = 'TINYINT';

elseif v_min_value >= -32768
AND v_max_value <= 32767 then v_recommended_type = 'SMALLINT';

elseif v_min_value >= -2147483648
AND v_max_value <= 2147483647 then v_recommended_type = 'INT';

elseif v_min_value >= -9223372036854775808
AND v_max_value <= 9223372036854775807 then v_recommended_type = 'BIGINT';

else v_recommended_type = numeric_rec.original_type;

/*Keep original if exceeds BIGINT*/
end if;

/*Update temp_column_analysis with recommended integer type and store max/min values for comment*/
UPDATE
    temp_column_analysis
SET
    recommended_type = v_recommended_type,
    max_octet_length = (v_max_value),
    min_octet_length = (v_min_value)
WHERE
    column_name = numeric_rec.column_name;

else
/*Has decimals, keep original type but format DECIMAL with precision/scale*/
if UPPER(numeric_rec.original_type) IN ('DECIMAL', 'NUMERIC')
AND numeric_rec.numeric_precision IS NOT NULL then if numeric_rec.numeric_scale IS NOT NULL
AND numeric_rec.numeric_scale > 0 then v_recommended_type = CONCAT(
    UPPER(numeric_rec.original_type),
    '(',
    numeric_rec.numeric_precision,
    ',',
    numeric_rec.numeric_scale,
    ')'
);

else v_recommended_type = CONCAT(
    UPPER(numeric_rec.original_type),
    '(',
    numeric_rec.numeric_precision,
    ')'
);

end if;

/*Update with formatted type and store min/max for comment*/
UPDATE
    temp_column_analysis
SET
    recommended_type = v_recommended_type,
    max_octet_length = (v_max_value),
    min_octet_length = (v_min_value)
WHERE
    column_name = numeric_rec.column_name;

else
/*For FLOAT, DOUBLE, or non-decimal types, just store min/max for comment*/
UPDATE
    temp_column_analysis
SET
    max_octet_length = (v_max_value),
    min_octet_length = (v_min_value)
WHERE
    column_name = numeric_rec.column_name;

end if;

end if;

end loop;

/*Cleanup numeric results table*/
DROP TEMPORARY TABLE IF EXISTS temp_numeric_results;

end if;

/*Build CREATE TABLE script*/
for script_rec in collect(script_query) loop
/*Add comma for non-first columns*/
if NOT v_first_column then v_column_list = CONCAT(v_column_list, ',\n');

else v_first_column = FALSE;

end if;

/*Build column definition*/
v_column_list = CONCAT(
    v_column_list,
    '    `' || script_rec.column_name || '` ' || script_rec.recommended_type
);

/*Add NOT NULL if applicable*/
if script_rec.is_nullable = 'NO' then v_column_list = CONCAT(v_column_list, ' NOT NULL');

end if;

/*Add DEFAULT if exists*/
if script_rec.column_default IS NOT NULL then if UPPER(script_rec.column_default) in ('CURRENT_TIMESTAMP', 'NULL') then v_column_list = CONCAT(
    v_column_list,
    ' DEFAULT ' || script_rec.column_default
);

else v_column_list = CONCAT(
    v_column_list,
    ' DEFAULT ''' || script_rec.column_default || ''''
);

end if;

end if;

/*Add EXTRA info (AUTO_INCREMENT, etc)*/
if script_rec.extra IS NOT NULL
AND script_rec.extra != '' then v_column_list = CONCAT(v_column_list, ' ' || script_rec.extra);

end if;

/*Add comment showing original type and optimization info*/
if script_rec.max_octet_length IS NOT NULL then
/*Check if it's a text column or numeric column*/
if UPPER(script_rec.original_type) in (
    'VARCHAR',
    'CHAR',
    'TEXT',
    'TINYTEXT',
    'MEDIUMTEXT',
    'LONGTEXT'
) then v_column_list = CONCAT(
    v_column_list,
    ' /* Original: ' || script_rec.original_column_type || ', Max Size: ' || script_rec.max_octet_length: > bigint || ' bytes */'
);

elseif UPPER(script_rec.original_type) in (
    'DECIMAL',
    'FLOAT',
    'DOUBLE',
    'NUMERIC',
    'TINYINT',
    'SMALLINT',
    'INT',
    'INTEGER',
    'MEDIUMINT',
    'BIGINT'
) then v_column_list = CONCAT(
    v_column_list,
    ' /* Original: ' || script_rec.original_column_type
);

if script_rec.numeric_scale IS NOT NULL then v_column_list = CONCAT(
    v_column_list,
    ', Decimal Places: ' || script_rec.numeric_scale
);

end if;

v_column_list = CONCAT(
    v_column_list,
    ', Min (*10): ' || script_rec.min_octet_length || ', Max (*10): ' || script_rec.max_octet_length || ' */'
);

end if;

end if;

end loop;

/*Add primary key if exists*/
if v_has_pk then v_column_list = CONCAT(
    v_column_list,
    ',\n    PRIMARY KEY (' || v_pk_columns || ')'
);

end if;

/*Add shard key if exists*/
if v_has_shard_key then v_column_list = CONCAT(
    v_column_list,
    ',\n    SHARD KEY (' || v_shard_key || ')'
);

end if;

/*Add sort key if exists*/
if v_has_sort_key then v_column_list = CONCAT(
    v_column_list,
    ',\n    SORT KEY (' || v_sort_key || ')'
);

end if;

/*Get other indexes/keys with column details*/
DROP TEMPORARY TABLE IF EXISTS temp_indexes;

CREATE TEMPORARY TABLE temp_indexes AS
SELECT
    INDEX_NAME,
    INDEX_TYPE,
    NON_UNIQUE,
    GROUP_CONCAT(
        COLUMN_NAME
        ORDER BY
            SEQ_IN_INDEX SEPARATOR '`, `'
    ) as columns
FROM
    INFORMATION_SCHEMA.STATISTICS
WHERE
    TABLE_SCHEMA = p_schema_name
    AND TABLE_NAME = p_table_name
    AND INDEX_NAME != 'PRIMARY'
    AND INDEX_TYPE NOT IN ('CLUSTERED COLUMNSTORE', 'SHARD')
GROUP BY
    INDEX_NAME,
    INDEX_TYPE,
    NON_UNIQUE
ORDER BY
    INDEX_NAME;

SELECT
    IFNULL(
        (
            SELECT
                GROUP_CONCAT(
                    CONCAT(
                        CASE
                            WHEN NON_UNIQUE = 0 THEN 'UNIQUE KEY '
                            WHEN INDEX_TYPE = 'FULLTEXT' THEN 'FULLTEXT KEY '
                            ELSE 'KEY '
                        END,
                        '`' || INDEX_NAME || '` (`' || columns || '`)'
                    )
                    ORDER BY
                        INDEX_NAME SEPARATOR ',\n    '
                )
            FROM
                temp_indexes
        ),
        NULL
    ) INTO v_keys_list;

DROP TEMPORARY TABLE IF EXISTS temp_indexes;

/*Add other keys if exist*/
if v_keys_list IS NOT NULL
AND v_keys_list != '' then v_column_list = CONCAT(v_column_list, ',\n    ' || v_keys_list);

end if;

/*Finalize CREATE statement*/
v_create_script = CONCAT(v_create_script, v_column_list || '\n)');

/*Add table options (copy from original table)*/
SELECT
    IFNULL(
        (
            SELECT
                CONCAT(
                    ' ENGINE=' || ENGINE || CASE
                        WHEN TABLE_COLLATION IS NOT NULL THEN CONCAT(' COLLATE=' || TABLE_COLLATION)
                        ELSE ''
                    END
                )
            FROM
                INFORMATION_SCHEMA.TABLES
            WHERE
                TABLE_SCHEMA = p_schema_name
                AND TABLE_NAME = p_table_name
            LIMIT
                1
        ), NULL
    ) INTO v_sql;

v_create_script = CONCAT(v_create_script, COALESCE(v_sql, '') || ';\n\n');

ECHO
SELECT
    v_create_script AS create_script;

/*      -- Output analysis results     ECHO SELECT '=== Column Analysis Results ===' AS info;     ECHO SELECT * FROM temp_column_analysis ORDER BY ordinal_position;          ECHO SELECT '\n=== Optimized CREATE TABLE Script ===' AS info;      -- Also output size comparison     ECHO SELECT '\n=== Size Optimization Summary ===' AS info;     ECHO SELECT          COUNT(*) AS total_text_columns,         SUM(CASE WHEN max_octet_length IS NOT NULL THEN 1 ELSE 0 END) AS analyzed_columns,         SUM(max_octet_length) AS total_data_size_bytes,         ROUND(SUM(max_octet_length) / 1024 / 1024, 2) AS total_data_size_mb     FROM temp_column_analysis     WHERE max_octet_length IS NOT NULL;          -- Cleanup     DROP TEMPORARY TABLE IF EXISTS temp_column_analysis; */
END;