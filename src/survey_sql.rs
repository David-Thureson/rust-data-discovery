use postgres::{Client, NoTls};
use util::rse;
use itertools::Itertools;

const PATH_SQL: &str = "C:/Projects/Rust/data/data-discovery/sql";
const FILE_NAME_CREATE_TABLES_POSTGRES: &str = "Survey Create Tables PostgreSQL.sql";
const TABLE_NAME_COLUMN: &str = "survey_column";
const TABLE_NAME_COLUMN_VALUE: &str = "survey_column_value";
const VALUE_NONE: &str = "{none}";
const VALUE_LINEFEED: &str = "{lf}";

pub fn main() {
}

pub enum SqlEngine {
    PostgreSql,
}

pub fn connect_client(connect: &str) -> Result<Client, String> {
    let client = rse!(Client::connect(connect, NoTls))?;
    Ok(client)
}

pub fn create_empty_survey(client: &mut Client) -> Result<(), String> {
    let sql_file = format!("{}/{}", PATH_SQL, FILE_NAME_CREATE_TABLES_POSTGRES);
    let sql = util::file::read_file_to_string_r(&sql_file).unwrap();
    //bg!(&sql);
    execute_sql(client, &sql)?;
    //rintln!("execute done");
    Ok(())
}

pub fn copy_data_from_spreadsheet(client: &mut Client, file_path: &str, start_row: usize, table_name: &str, column_names: &str) -> Result<(), String> {
    let sql = format!("drop table if exists {}", table_name);
    execute_sql(client, &sql)?;

    let column_names = split_column_names_postgres(column_names);
    let sql_columns = column_names.iter().join(", ");

    let content = util::file::read_file_to_string_remove_bom_chars_r(file_path)?;
    let mut lines = content.split("\r\n").collect::<Vec<_>>();
    let skip_rows = start_row - 1;
    for _ in 0..skip_rows {
        lines.remove(0);
    }

    let mut sql_inserts = "".to_string();
    let mut max_lengths = Vec::with_capacity(column_names.len());
    for _ in 0..column_names.len() {
        max_lengths.push(0);
    }
    for (line_index, line) in lines.iter().enumerate() {
        let cells = line.split('\t').take(column_names.len()).collect::<Vec<_>>();
        //bg!(line_index, cells.len(), &line);
        //ebug_cells(&cells);
        assert_eq!(cells.len(), column_names.len());
        let values = cells.iter().enumerate()
            .map(|(cell_index, cell)| {
                let cell = cleanup_cell(cell);
                let cell = cell.replace("'", "''");
                if cell.is_empty() {
                    "null".to_string()
                } else {
                    max_lengths[cell_index] = max_lengths[cell_index].max(cell.len());
                    format!("'{}'", cell)
                }
            })
            .join(", ");
        let sql_one_insert = format!("\ninsert into {} ({}) values ({});", table_name, sql_columns, values);
        sql_inserts.push_str(&sql_one_insert);
    }

    let mut sql_create_table = format!("create table {} (", table_name);
    for (index, column_name) in column_names.iter().enumerate() {
        let length = get_higher_step_number(max_lengths[index]);
        let comma = if index == column_names.len() - 1 { "" } else { "," };
        sql_create_table.push_str(&format!("\n\t{} varchar({}) null{}", column_name, length, comma));
    }
    sql_create_table.push_str(");");

    dbg!(&sql_create_table);
    execute_sql(client, &sql_create_table)?;

    dbg!(&sql_inserts);
    execute_sql(client, &sql_inserts)?;

    Ok(())
}

fn execute_sql(client: &mut Client, sql: &str) -> Result<(), String> {
    rse!(client.batch_execute(&sql))
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

pub fn split_column_names_postgres(columns: &str) -> Vec<String> {
    let columns = columns.split('\t').map(|col| col.to_string()).collect::<Vec<_>>();
    for column in columns.iter() {
        if column.contains(" ") {
            panic!("Column \"{}\" contains a space.", column);
        }
        if column.contains("-") {
            panic!("Column \"{}\" contains a hyphen.", column);
        }
    }
    columns
}

fn cleanup_cell(cell: &str) -> String {
    let cell = util::parse::remove_delimiters(cell, "\"", "\"");
    let cell = util::parse::remove_delimiters(cell, "'", "'");
    let cell = cell.trim();
    let cell = cell.replace("\n", VALUE_LINEFEED);
    cell.to_string()
}

fn get_higher_step_number(value: usize) -> usize {
    let steps = [10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000];
    for step in steps {
        if value <= step {
            return step;
        }
    }
    panic!()
}

#[allow(dead_code)]
fn debug_cells(cells: &Vec<&str>) {
    println!();
    for (index, cell) in cells.iter().enumerate() {
        println!("[{}]\t\"{}\"", index, cell);
    }
    println!();
}