//! Support for binary-format `COPY` query execution with rust-postgres.
//!
//! # Example
//!
//! ```rust,no_run
//! extern crate postgres;
//! extern crate postgres_binary_copy;
//!
//! use postgres::{Connection, SslMode};
//! use postgres::types::{Type, ToSql};
//! use postgres_binary_copy::BinaryCopyReader;
//!
//! fn main() {
//!     let conn = Connection::connect("postgres://postgres@localhost",
//!                                    &SslMode::None).unwrap();
//!
//!     conn.execute("CREATE TABLE foo (id INT PRIMARY KEY, bar VARCHAR)", &[])
//!         .unwrap();
//!
//!     let types = &[Type::Int4, Type::Varchar];
//!     let data: Vec<Box<ToSql>> = vec![Box::new(1i32), Box::new("hello"),
//!                                      Box::new(2i32), Box::new("world")];
//!     let data = data.iter().map(|v| &**v);
//!     let mut reader = BinaryCopyReader::new(types, data);
//!
//!     let stmt = conn.prepare("COPY foo (id, bar) FROM STDIN BINARY").unwrap();
//!     stmt.copy_in(&[], &mut reader).unwrap();
//! }
//! ```
#![doc(html_root_url="https://sfackler.github.io/rust-postgres-binary-copy/doc/v0.1.0")]
#![warn(missing_docs)]
extern crate byteorder;
extern crate postgres;

use byteorder::{BigEndian, WriteBytesExt};
use postgres::error::Error;
use postgres::types::{Type, ToSql, SessionInfo, IsNull, ReadWithInfo};
use std::error;
use std::io::prelude::*;
use std::io::{self, Cursor};

/// Like `Iterator`, except that it returns borrowed values.
///
/// In constrast to `Iterator<Item = &T>`, a type implementing
/// `StreamingIterator<Item = T>` does not need to have all of the values it
/// returns in memory at the same time.
///
/// All `Iterator`s over `&T` are also `StreamingIterator`s over `T`.
pub trait StreamingIterator {
    /// The type of elements being iterated.
    type Item: ?Sized;

    /// Advances the iterator and returns the next value.
    ///
    /// Returns `None` when the end is reached.
    fn next(&mut self) -> Option<&Self::Item>;
}

impl<'a, T: 'a + ?Sized, I: Iterator<Item = &'a T>> StreamingIterator for I {
    type Item = T;

    fn next(&mut self) -> Option<&T> {
        unsafe { std::mem::transmute(Iterator::next(self)) }
    }
}

#[derive(Debug, Copy, Clone)]
enum ReadState {
    Header,
    Body(usize),
    Footer,
}

/// A `ReadWithInfo` implementation that generates binary-formatted output
/// for use with binary-format `COPY FROM STDIN` statements.
#[derive(Debug)]
pub struct BinaryCopyReader<'a, I> {
    types: &'a [Type],
    state: ReadState,
    it: I,
    buf: Cursor<Vec<u8>>,
}

impl<'a, I> BinaryCopyReader<'a, I> where I: StreamingIterator<Item = ToSql> {
    /// Creates a new `BinaryCopyReader`.
    ///
    /// The reader will output tuples with a structure described by `types` and
    /// values from `it`. `it` should return values in row-major order.
    pub fn new(types: &'a [Type], it: I) -> BinaryCopyReader<'a, I> {
        let mut buf = vec![];
        let _ = buf.write(b"PGCOPY\n\xff\r\n\0");
        let _ = buf.write_i32::<BigEndian>(0);
        let _ = buf.write_i32::<BigEndian>(0);

        BinaryCopyReader {
            types: types,
            state: ReadState::Header,
            it: it,
            buf: Cursor::new(buf),
        }
    }

    fn fill_buf(&mut self, info: &SessionInfo) -> io::Result<()> {
        enum Op<'a> {
            Value(usize, &'a ToSql),
            Footer,
            Nothing,
        }

        let op = match (self.state, self.it.next()) {
            (ReadState::Header, Some(value)) => {
                self.state = ReadState::Body(0);
                Op::Value(0, value)
            }
            (ReadState::Body(old_idx), Some(value)) => {
                let idx = (old_idx + 1) % self.types.len();
                self.state = ReadState::Body(idx);
                Op::Value(idx, value)
            }
            (ReadState::Header, None) | (ReadState::Body(_), None) => {
                self.state = ReadState::Footer;
                Op::Footer
            }
            (ReadState::Footer, _) => Op::Nothing,
        };

        self.buf.set_position(0);
        self.buf.get_mut().clear();

        match op {
            Op::Value(idx, value) => {
                if idx == 0 {
                    let len = self.types.len();
                    let len = if len > i16::max_value() as usize {
                        let err: Box<error::Error+Sync+Send> =
                            "value too large to transmit".into();
                        return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                                  Error::Conversion(err)));
                    } else {
                        len as i16
                    };
                    let _ = self.buf.write_i16::<BigEndian>(len);
                }

                let len_pos = self.buf.position();
                let _ = self.buf.write_i32::<BigEndian>(0); // space for length
                let len = match value.to_sql_checked(&self.types[idx], &mut self.buf, info) {
                    Ok(IsNull::Yes) => -1,
                    Ok(IsNull::No) => {
                        let len = self.buf.position() - 4 - len_pos;
                        if len > i32::max_value() as u64 {
                            let err: Box<error::Error+Sync+Send> =
                                "value too large to transmit".into();
                            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                                      Error::Conversion(err)));
                        } else {
                            len as i32
                        }
                    }
                    Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
                };
                self.buf.set_position(len_pos);
                let _ = self.buf.write_i32::<BigEndian>(len);
            }
            Op::Footer => {
                let _ = self.buf.write_i16::<BigEndian>(-1);
            }
            Op::Nothing => {}
        }

        self.buf.set_position(0);
        Ok(())
    }
}

impl<'a, I> ReadWithInfo for BinaryCopyReader<'a, I> where I: StreamingIterator<Item = ToSql> {
    fn read_with_info(&mut self, buf: &mut [u8], info: &SessionInfo) -> io::Result<usize> {
        if self.buf.position() == self.buf.get_ref().len() as u64 {
            try!(self.fill_buf(info));
        }
        self.buf.read(buf)
    }
}

#[cfg(test)]
mod test {
    use super::BinaryCopyReader;
    use postgres::{Connection, SslMode};
    use postgres::types::{Type, ToSql};

    #[test]
    fn basic() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY, bar VARCHAR)", &[]).unwrap();

        let stmt = conn.prepare("COPY foo (id, bar) FROM STDIN BINARY").unwrap();

        let types = &[Type::Int4, Type::Varchar];
        let values: Vec<Box<ToSql>> = vec![Box::new(1i32), Box::new("foobar"),
                                           Box::new(2i32), Box::new(None::<String>)];
        let values = values.iter().map(|e| &**e);
        let mut reader = BinaryCopyReader::new(types, values);

        stmt.copy_in(&[], &mut reader).unwrap();

        let stmt = conn.prepare("SELECT id, bar FROM foo ORDER BY id").unwrap();
        assert_eq!(vec![(1i32, Some("foobar".to_string())), (2i32, None)],
                   stmt.query(&[])
                        .unwrap()
                        .into_iter()
                        .map(|r| (r.get(0), r.get(1)))
                        .collect::<Vec<(i32, Option<String>)>>());
    }

    #[test]
    fn many_rows() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY, bar VARCHAR)", &[]).unwrap();

        let stmt = conn.prepare("COPY foo (id, bar) FROM STDIN BINARY").unwrap();

        let types = &[Type::Int4, Type::Varchar];
        let mut values: Vec<Box<ToSql>> = vec![];
        for i in 0..10000i32 {
            values.push(Box::new(i));
            values.push(Box::new(format!("the value for {}", i)));
        }

        let values = values.iter().map(|e| &**e);
        let mut reader = BinaryCopyReader::new(types, values);

        stmt.copy_in(&[], &mut reader).unwrap();

        let stmt = conn.prepare("SELECT id, bar FROM foo ORDER BY id").unwrap();
        let result = stmt.query(&[]).unwrap();
        assert_eq!(10000, result.len());
        for (i, row) in result.into_iter().enumerate() {
            assert_eq!(i as i32, row.get(0));
            assert_eq!(format!("the value for {}", i), row.get::<_, String>(1));
        }
    }

    #[test]
    fn big_rows() {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY, bar BYTEA)", &[]).unwrap();

        let stmt = conn.prepare("COPY foo (id, bar) FROM STDIN BINARY").unwrap();

        let types = &[Type::Int4, Type::Bytea];
        let mut values: Vec<Box<ToSql>> = vec![];
        for i in 0..2i32 {
            values.push(Box::new(i));
            values.push(Box::new(vec![i as u8; 128 * 1024]));
        }

        let values = values.iter().map(|e| &**e);
        let mut reader = BinaryCopyReader::new(types, values);

        stmt.copy_in(&[], &mut reader).unwrap();

        let stmt = conn.prepare("SELECT id, bar FROM foo ORDER BY id").unwrap();
        let result = stmt.query(&[]).unwrap();
        assert_eq!(2, result.len());
        for (i, row) in result.into_iter().enumerate() {
            assert_eq!(i as i32, row.get(0));
            assert_eq!(vec![i as u8; 128 * 1024], row.get::<_, Vec<u8>>(1));
        }
    }
}
