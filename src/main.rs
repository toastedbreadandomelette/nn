use parser::CsvParser;
use vector::Vector;

use crate::cell::Cell;

// extern crate vector;

mod cell;
mod dframe;
mod iter;
mod parse_state;
mod parser;

#[allow(unused_variables)]
fn main() {
    // Some of the local files in author's computer
    let str =
        "overseas-trade-indexes-September-2022-quarter-provisional-csv.csv";
    let str2 = "../playground/ML_AI/ML/Projects/modified_array/Data8277.csv";
    let str3 = "Data7602DescendingYearOrder.csv";
    // let str4 = "sample.csv";

    let t = std::time::Instant::now();
    let fd = CsvParser::parse_multi_threaded(str3, 12);
    println!("Time: {}ms {}", t.elapsed().as_millis(), fd.len());

    fd.iter().take(20).for_each(|c| println!("{:?}", c));

    match fd.iter_col("Year") {
        Some(iter) => {
            let values = iter
                .enumerate()
                .filter(
                    |(index, c)| {
                        if let Cell::String(v) = c {
                            true
                        } else {
                            false
                        }
                    },
                )
                .map(|c| (c.0, c.1.clone()))
                .collect::<Vector<(usize, Cell)>>();

            // println!("{} {:?}", values.len(), &values[0..]);
        }
        None => {}
    };

    println!("{:?}\n{:?}", &fd.header(), &fd.dtypes());
}
