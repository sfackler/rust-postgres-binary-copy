# postgres-binary-copy

[![Build Status](https://travis-ci.org/sfackler/rust-postgres-binary-copy.svg?branch=master)](https://travis-ci.org/sfackler/rust-postgres-binary-copy)

Support for binary-format `COPY` query execution with
[rust-postgres](https://github.com/sfackler/rust-postgres).

## Example

```rust
extern crate postgres;
extern crate postgres_binary_copy;

use postgres::{Connection, SslMode};
use postgres::types::{Type, ToSql};
use postgres_binary_copy::BinaryCopyReader;

fn main() {
    let conn = Connection::connect("postgres://postgres@localhost",
                                   &SslMode::None).unwrap();

    conn.execute("CREATE TABLE foo (id INT PRIMARY KEY, bar VARCHAR)", &[])
        .unwrap();

    let types = &[Type::Int4, Type::Varchar];
    let data: Vec<Box<ToSql>> = vec![Box::new(1i32), Box::new("hello"),
                                     Box::new(2i32), Box::new("world")];
    let data = data.iter().map(|v| &**v);
    let mut reader = BinaryCopyReader::new(types, data);

    let stmt = conn.prepare("COPY foo (id, bar) FROM STDIN BINARY").unwrap();
    stmt.copy_in(&[], &mut reader).unwrap();
}
```
