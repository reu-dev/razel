# razel internals

### Sandbox

paths used to execute commands:

| Executor      | cwd                      | executable | data              | non-data inputs                 | outputs                   |
|---------------|--------------------------|------------|-------------------|---------------------------------|---------------------------|
| CustomCommand | command specific tmp dir | symlink    | rel path, symlink | `razel-bin/`, rel path, symlink | `razel-bin/`, copied back |
| Task          | -                        | -          | rel path          | `razel-bin/`, rel path          | `razel-bin/`              |
