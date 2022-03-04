use postgres::{Client, NoTls};
use util::rse;

const PATH_SQL: &str = "C:/Projects/Rust/data/data-discovery/sql";
const FILE_NAME_CREATE_TABLES_POSTGRES: &str = "Survey Create Tables PostgreSQL.sql";
const TABLE_NAME_COLUMN: &str = "survey_column";
const TABLE_NAME_COLUMN_VALUE: &str = "survey_column_value";
const VALUE_NONE: &str = "{none}";

pub fn main() {
}

pub enum SqlEngine {
    PostgreSql,
}

pub fn create_empty_survey(connect: &str) -> Result<(), String> {
    let mut client = rse!(Client::connect(connect, NoTls))?;
    //rintln!("client created");

    let sql_file = format!("{}/{}", PATH_SQL, FILE_NAME_CREATE_TABLES_POSTGRES);
    let sql = util::file::read_file_to_string_r(&sql_file).unwrap();
    //bg!(&sql);
    rse!(client.batch_execute(&sql))?;
    //rintln!("execute done");

    Ok(())
}

/*
pub fn connect_postgresql(connect: &str) -> Result<Client, String> {
    match Client::connect(connect, NoTls) {
        Ok(client) => {
            return Ok(client);
        },
        Err(e) =>
    }


    client
}
*/

pub fn gen_fill_columns(columns: &[(&str, &str)]) {
    for (table, column) in columns.iter() {
        let where_clause = format!("where (table_name = '{}') and (column_name = '{}')", table, column);
        let non_empty_clause = format!("({} is not null) and (length(trim({})) > 0)", column, column);
        let coalesce_clause = format!("coalesce(trim({}), '')", column);
        println!("update {} set null_count = (select count(*) from {} where ({} is null)) {};",
                 TABLE_NAME_COLUMN, table, column, where_clause);
        println!("update {} set blank_count = (select count(*) from {} where (length(trim({})) = 0)) {};",
                 TABLE_NAME_COLUMN, table, column, where_clause);
        println!("update {} set trim_count = (select count(*) from {} where (length({}) <> length(trim({})))) {};",
                 TABLE_NAME_COLUMN, table, column, column, where_clause);
        println!("update {} set distinct_count = (select count(distinct {}) from {}) {};",
                 TABLE_NAME_COLUMN, coalesce_clause, table, where_clause);
        println!("update {} set min_length = (select min(length(trim({}))) from {} where {}) {};",
                 TABLE_NAME_COLUMN, column, table, non_empty_clause, where_clause);
        println!("update {} set max_length = (select max(length(trim({}))) from {}) {};",
                 TABLE_NAME_COLUMN, column, table, where_clause);
    }
}

pub fn gen_fill_column_values(max_value_count: usize, columns: &[(&str, &str)]) {
    println!("delete from {} where true;", TABLE_NAME_COLUMN_VALUE);
    for (table, column) in columns.iter() {
        let inner_select = format!("select case when length(trim(coalesce({}, ''))) = 0 then '{}' else trim({}) end as value from {}",
                                   column, VALUE_NONE, column, table);
        let select = format!("select '{}', '{}', a.value, count(*) from ({}) as a group by a.value order by count(*) desc limit {}",
                             table, column, inner_select, max_value_count);
        println!("insert into {} (table_name, column_name, value, value_count) {};",
                 TABLE_NAME_COLUMN_VALUE, select);
    }
}

