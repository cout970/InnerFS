use std::time::{SystemTime, UNIX_EPOCH};

pub fn humanize_bytes_binary(value: usize) -> String {
    use ::core::fmt::Write;
    let mut num_bytes = value as f64;
    let mut result = String::new();
    if num_bytes < 0.0 {
        write!(result, "-").unwrap();
        num_bytes = -num_bytes;
    }

    if num_bytes < 1024.0 {
        write!(result, "{} B", num_bytes as u16).unwrap();
        result
    } else {
        const SUFFIX: [&str; 11] = [
            "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB", "RiB", "QiB",
        ];
        const UNIT: f64 = 1024.0;
        let base = num_bytes.log2() as usize / 10;
        let curr_base = UNIT.powi(base as i32);
        let units = num_bytes / curr_base;
        let units = (units * 100.0).floor() / 100.0;
        let mut once = true;
        let mut extra = String::new();
        write!(extra, "{:.2}", units).unwrap();
        let trimmed = extra
            .trim_end_matches(|_| {
                if once {
                    once = false;
                    true
                } else {
                    false
                }
            })
            .trim_end_matches("0")
            .trim_end_matches(".");
        result.push_str(trimmed);
        result.push_str(" ");
        result.push_str(SUFFIX[base]);
        result
    }
}

pub fn current_timestamp() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

pub fn ask_for_confirmation(msg: &str) -> bool {
    println!("--------------------------------------------------------------------------------");
    println!(" > {}", msg);
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
    let choice = input.trim().to_ascii_lowercase();
    choice == "yes" || choice == "y"
}