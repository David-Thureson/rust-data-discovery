-- Survey Create Tables PostgresSQL.sql

drop view if exists v_survey_column_value;
drop view if exists v_survey_column;
drop view if exists v_survey_table;
drop table if exists survey_column_value;
drop table if exists survey_column;
drop table if exists survey_table;

create table survey_table (
    database_name varchar(100) not null,
    table_name varchar(100) not null,
    row_count integer not null
);

create table survey_column (
    database_name varchar(100) not null,
    table_name varchar(100) not null,
    column_name varchar(100) not null,
    sequence integer not null,
    general_type varchar(100) not null,
    sql_type varchar(100) null,
    null_count integer null,
    blank_count integer null,
    trim_count integer null,
    distinct_count integer null,
    min_length integer null,
    max_length integer null
);

create table survey_column_value (
    database_name varchar(100) not null,
    table_name varchar(100) not null,
    column_name varchar(100) not null,
    value varchar(100) not null,
    value_count integer not null
);
