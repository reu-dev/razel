use std::path::{Component, Path, PathBuf};

/// Drops ../ components from paths without disk read
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(Component::Prefix(c)) = components.peek() {
        let buf = PathBuf::from(c.as_os_str());
        components.next();
        buf
    } else {
        PathBuf::new()
    };
    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

/// Replaces `path` with `new_path` in all args, where it is the full arg or delimited by `=`, `,`, `;` or `"`.
pub fn replace_path_in_args(args: &mut [String], path: &str, new_path: &str) {
    for arg in args.iter_mut() {
        replace_path_in_arg(arg, path, new_path);
    }
}

fn replace_path_in_arg(arg: &mut String, path: &str, new_path: &str) {
    if path.is_empty() || path == new_path {
        return;
    }
    if arg == path {
        new_path.clone_into(arg);
        return;
    }
    // delimiters are ASCII, bytes of multi-byte chars never match
    let is_delimiter = |b: Option<&u8>| b.is_none_or(|b| matches!(b, b'=' | b',' | b';' | b'"'));
    let bytes = arg.as_bytes();
    let mut result = String::new();
    let mut last = 0;
    for (start, _) in arg.match_indices(path) {
        let end = start + path.len();
        if is_delimiter(start.checked_sub(1).map(|i| &bytes[i])) && is_delimiter(bytes.get(end)) {
            if result.is_empty() {
                result.reserve(arg.len() + new_path.len());
            }
            result.push_str(&arg[last..start]);
            result.push_str(new_path);
            last = end;
        }
    }
    if last != 0 {
        result.push_str(&arg[last..]);
        *arg = result;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_normalize_path(path: &str, exp: &str) {
        let act = super::normalize_path(Path::new(path))
            .to_string_lossy()
            .to_string();
        assert_eq!(act, exp);
    }

    #[test]
    fn normalize_path() {
        if cfg!(windows) {
            test_normalize_path("file", "file");
            test_normalize_path("C:/Users/user", "C:\\Users\\user");
            test_normalize_path("C:/Users/user/../file", "C:\\Users\\file");
        } else {
            test_normalize_path("file", "file");
            test_normalize_path("/home/user", "/home/user");
            test_normalize_path("/home/user/../file", "/home/file");
        }
    }

    fn test_replace_path_in_arg(arg: &str, exp: &str) {
        let mut act = arg.to_string();
        super::replace_path_in_arg(&mut act, "a.txt", "razel-out/a.txt");
        assert_eq!(act, exp);
    }

    #[test]
    fn replace_path_in_arg() {
        test_replace_path_in_arg("a.txt", "razel-out/a.txt");
        test_replace_path_in_arg("--out=a.txt", "--out=razel-out/a.txt");
        test_replace_path_in_arg("a.txt,b.txt;a.txt", "razel-out/a.txt,b.txt;razel-out/a.txt");
        test_replace_path_in_arg(
            r#"{"in":"a.txt","x":["a.txt"]}"#,
            r#"{"in":"razel-out/a.txt","x":["razel-out/a.txt"]}"#,
        );
        // mixed delimiters
        test_replace_path_in_arg(
            r#"{"args":"--out=a.txt"}"#,
            r#"{"args":"--out=razel-out/a.txt"}"#,
        );
        test_replace_path_in_arg("--out=\"a.txt\"", "--out=\"razel-out/a.txt\"");
        test_replace_path_in_arg("ä=a.txt", "ä=razel-out/a.txt");
        // not delimited
        test_replace_path_in_arg("dir/a.txt", "dir/a.txt");
        test_replace_path_in_arg("ba.txt", "ba.txt");
        test_replace_path_in_arg("a.txt.bak", "a.txt.bak");
        test_replace_path_in_arg("-oa.txt", "-oa.txt");
        test_replace_path_in_arg("/OUT:a.txt", "/OUT:a.txt");
        test_replace_path_in_arg("cat a.txt", "cat a.txt");
        // already replaced
        test_replace_path_in_arg("razel-out/a.txt", "razel-out/a.txt");
    }
}
