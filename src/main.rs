use parser::CsvParser;

// extern crate vector;

mod cell;
mod dframe;
mod iter;
mod parse_state;
mod parser;

#[allow(unused_variables)]
fn main() {
    // Some of the local files in author's computer
    let str1 =
        "overseas-trade-indexes-September-2022-quarter-provisional-csv.csv";
    let str2 = "~/Data8277.csv";
    let str3 = "Data7602DescendingYearOrder.csv";
    let str4 = "sample.csv";

    let t = std::time::Instant::now();
    let fd = CsvParser::parse_multi_threaded(str3, 12);
    println!("Time: {}ms {}", t.elapsed().as_millis(), fd.len());

    fd.iter().take(20).for_each(|c| println!("{:?}", c));

    println!("{:?}\n{:?}", &fd.header(), &fd.dtypes());
}
