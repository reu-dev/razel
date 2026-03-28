pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_099_511_627_776 {
        format!("{:.1} TiB", bytes as f64 / 1_099_511_627_776.0)
    } else if bytes >= 1_073_741_824 {
        format!("{:.1} GiB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MiB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1_024 {
        format!("{:.1} KiB", bytes as f64 / 1_024.0)
    } else {
        format!("{} B", bytes)
    }
}

pub fn format_secs(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1_023), "1023 B");
        assert_eq!(format_bytes(1_024), "1.0 KiB");
        assert_eq!(format_bytes(1_536), "1.5 KiB");
        assert_eq!(format_bytes(1_048_575), "1024.0 KiB");
        assert_eq!(format_bytes(1_048_576), "1.0 MiB");
        assert_eq!(format_bytes(1_258_291), "1.2 MiB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GiB");
        assert_eq!(format_bytes(2_684_354_560), "2.5 GiB");
        assert_eq!(format_bytes(1_099_511_627_776), "1.0 TiB");
        assert_eq!(format_bytes(4_070_197_023_129), "3.7 TiB");
    }

    #[test]
    fn test_format_secs() {
        assert_eq!(format_secs(0), "0s");
        assert_eq!(format_secs(59), "59s");
        assert_eq!(format_secs(60), "1m 00s");
        assert_eq!(format_secs(90), "1m 30s");
        assert_eq!(format_secs(3599), "59m 59s");
        assert_eq!(format_secs(3600), "1h 00m");
        assert_eq!(format_secs(3661), "1h 01m");
        assert_eq!(format_secs(7384), "2h 03m");
    }
}
