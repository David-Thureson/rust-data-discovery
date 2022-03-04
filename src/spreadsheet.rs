use itertools::Itertools;
use super::survey_sql::split_column_names_postgres;

pub fn main() {
}

pub fn spreadsheet_to_sql_survey() {

}

pub fn gen_formula_sql_insert(table_name: &str, column_list_ref: &str, start_row: usize, columns: &str) -> String {
    let columns= split_column_names_postgres(columns);
    let column_list = columns.iter()
        .join(", ");
    let column_refs = get_column_letters(columns.len() as u8).iter()
        .map(|column_ref| {
            format!("{}{}", column_ref, start_row)
            // format!("if(isblank({}),\"null\",concatenate(\"'\", substitute({},\"'\",\"''\"), \"'\"))", column_ref, column_ref)
        })
        // .join("");
        .join(",\", \",");
    println!("insert into {} ({}) values (", table_name, column_list);
    let formula = format!("=concatenate({},{},\");\")", column_list_ref, column_refs);
    formula
}

pub fn gen_pg_create_table(table_name: &str, columns: &str) {
    let columns= split_column_names_postgres(columns);
    println!("\ncreate table {} (", table_name);
    for (index, column_name) in columns.iter().enumerate() {
        let comma = if index == columns.len() - 1 { "" } else { "," };
        println!("\t{} varchar(100){}", column_name, comma);
    }
    println!(")\n");
}

fn get_column_letters(column_count: u8) -> Vec<String> {
    assert!(column_count <= 52);
    let mut list = vec![];
    for i in 0..column_count {
        let (prefix, index) = if i < 26 {
            ("", i)
        } else {
            ("A", i - 26)
        };
        let c = (index + 65) as char;
        list.push(format!("{}{}", prefix, c));
    }
    list
}

