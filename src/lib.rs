extern crate postgres;
extern crate itertools;

pub mod spreadsheet;

pub mod survey_sql;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
