-- Survey Create Views PostgreSQL.sql

create or replace view v_survey_column as
select
        col.database_name,
        col.table_name,
        col.column_name,
        col.sequence,
        col.general_type,
        col.sql_type,
        tbl.row_count,
        col.null_count + col.blank_count as empty_count,
        ((col.null_count + col.blank_count) * 10000) / (tbl.row_count * 100) as empty_pct,
        col.null_count,
        (col.null_count * 10000) / (tbl.row_count * 100) as null_pct,
        col.blank_count,
        (col.blank_count * 10000) / (tbl.row_count * 100) as blank_pct,
        col.trim_count,
        (col.trim_count * 10000) / (tbl.row_count * 100) as trim_pct,
        col.distinct_count,
        col.distinct_count = tbl.row_count as is_distinct,
        col.min_length,
        col.max_length
    from survey_column col
        left outer join survey_table tbl on (tbl.database_name = col.database_name) and (tbl.table_name = col.table_name)
    order by col.database_name, col.table_name, col.sequence;

create or replace view v_survey_column_value as
select
        val.database_name,
        val.table_name,
        val.column_name,
        col.row_count,
        val.value,
        val.value_count,
        (val.value_count * 10000) / (col.row_count * 100) as value_pct
    from survey_column_value val
        left outer join v_survey_column col on (col.database_name = val.database_name) and (col.table_name = val.table_name) and (col.column_name = val.column_name)
    order by val.database_name, val.table_name, col.sequence, val.value_count desc;

