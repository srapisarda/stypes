use std::{error::Error, io, process};
use csv;


fn example() -> Result<(), Box<dyn Error>> {
    // Build the CSV reader and iterate over each record.
    let mut reader = csv::Reader::from_reader(io::stdin());

    let headers = reader.headers()?;

    println!("Headers: {:?}", headers);

    for result in reader.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result?;
        let mut line: String= "".to_string();
        for n in 14..18 {
            let formatted = get_formatted(record[n].to_string());
            line = format!("{}{}", line, formatted);
        }

        println!("{}\\\\", line.as_str());
    }
    Ok(())
}

fn get_formatted(val: String) -> String {
    if val.is_empty(){
        return "&-".to_string();
    }
    let int_value =  val.parse::<i32>().unwrap();
    if int_value < 0 {
        return  format!("{} {} {}", "&", "\\cellcolor[HTML]{00FF00}", val);
    }
    if int_value == 0  {
        return  format!("{} {}", "&", val);
    }
    if int_value <= 20 {
       return format!("{} {} {}", "&", "\\cellcolor[HTML]{FFCC67}", val);
    }
    return format!("{} {} {}", "&", "\\cellcolor[HTML]{FF5000}", val);
}

fn main() {
    if let Err(err) = example() {
        println!("error running example: {}", err);
        process::exit(1);
    }
}

