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
//!                                    SslMode::None).unwrap();
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
//!     let stmt = conn.prepare("COPY foo (id, bar) FROM STDIN (FORMAT binary)").unwrap();
//!     stmt.copy_in(&[], &mut reader).unwrap();
//! }
//! ```
#![doc(html_root_url="https://sfackler.github.io/rust-postgres-binary-copy/doc/v0.2.1")]
#![warn(missing_docs)]
extern crate byteorder;
extern crate postgres;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use postgres::error::Error;
use postgres::types::{Type, ToSql, IsNull};
use postgres::stmt::{CopyInfo, ReadWithInfo, WriteWithInfo};
use std::cmp;
use std::error;
use std::fmt;
use std::io::prelude::*;
use std::io::{self, Cursor};
use std::mem;

const HEADER_MAGIC: &'static [u8] = b"PGCOPY\n\xff\r\n\0";

/// Like `Iterator`, except that it returns borrowed values.
///
/// In contrast to `Iterator<Item = &T>`, a type implementing
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
/// for use with `COPY ... FROM STDIN (FORMAT binary)` statements.
pub struct BinaryCopyReader<'a, I> {
    types: &'a [Type],
    state: ReadState,
    it: I,
    buf: Cursor<Vec<u8>>,
}

impl<'a, I> fmt::Debug for BinaryCopyReader<'a, I>
    where I: fmt::Debug
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("BinaryCopyReader")
           .field("types", &self.types)
           .field("state", &self.state)
           .field("it", &self.it)
           .finish()
    }
}

impl<'a, I> BinaryCopyReader<'a, I>
    where I: StreamingIterator<Item = ToSql>
{
    /// Creates a new `BinaryCopyReader`.
    ///
    /// The reader will output tuples with a structure described by `types` and
    /// values from `it`. `it` should return values in row-major order.
    pub fn new(types: &'a [Type], it: I) -> BinaryCopyReader<'a, I> {
        let mut buf = vec![];
        let _ = buf.write(HEADER_MAGIC);
        let _ = buf.write_i32::<BigEndian>(0);
        let _ = buf.write_i32::<BigEndian>(0);

        BinaryCopyReader {
            types: types,
            state: ReadState::Header,
            it: it,
            buf: Cursor::new(buf),
        }
    }

    fn fill_buf(&mut self, info: &CopyInfo) -> io::Result<()> {
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
                        let err: Box<error::Error + Sync + Send> = "value too large to transmit"
                                                                       .into();
                        return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                                  Error::Conversion(err)));
                    } else {
                        len as i16
                    };
                    let _ = self.buf.write_i16::<BigEndian>(len);
                }

                let len_pos = self.buf.position();
                let _ = self.buf.write_i32::<BigEndian>(0); // space for length
                let len = match value.to_sql_checked(&self.types[idx],
                                                     &mut self.buf,
                                                     &info.session_info()) {
                    Ok(IsNull::Yes) => -1,
                    Ok(IsNull::No) => {
                        let len = self.buf.position() - 4 - len_pos;
                        if len > i32::max_value() as u64 {
                            let err: Box<error::Error + Sync + Send> = "value too large to \
                                                                        transmit"
                                                                           .into();
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

impl<'a, I> ReadWithInfo for BinaryCopyReader<'a, I>
    where I: StreamingIterator<Item = ToSql>
{
    fn read_with_info(&mut self, buf: &mut [u8], info: &CopyInfo) -> io::Result<usize> {
        if self.buf.position() == self.buf.get_ref().len() as u64 {
            try!(self.fill_buf(info));
        }
        self.buf.read(buf)
    }
}

/// A `Read`er passed to `WriteValue::write_value`.
pub struct WriteValueReader<'a>(&'a mut &'a [u8]);

impl<'a> Read for WriteValueReader<'a> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

/// A trait for types that can receive values from a `BinaryCopyWriter`.
///
/// It is implemented for all `FnMut(Option<&mut WriteValueReader>, &CopyInfo)
/// -> io::Result<()>` closures.
pub trait WriteValue {
    /// Processes a SQL value.
    fn write_value(&mut self, r: &mut WriteValueReader, info: &CopyInfo) -> io::Result<()>;

    /// Processes a `NULL` SQL value.
    fn write_null_value(&mut self, info: &CopyInfo) -> io::Result<()>;
}

impl<F> WriteValue for F
    where F: FnMut(Option<&mut WriteValueReader>, &CopyInfo) -> io::Result<()>
{
    fn write_value(&mut self, r: &mut WriteValueReader, info: &CopyInfo) -> io::Result<()> {
        self(Some(r), info)
    }

    fn write_null_value(&mut self, info: &CopyInfo) -> io::Result<()> {
        self(None, info)
    }
}

#[derive(Debug)]
enum WriteState {
    AtHeader,
    AtTuple,
    AtFieldSize(usize),
    AtField {
        size: usize,
        remaining: usize,
    },
    Done,
}

/// A `ReadWithInfo` implementation that processes binary-formatted input
/// for use with `COPY ... TO STDOUT (FORMAT binary)` statements.
pub struct BinaryCopyWriter<W> {
    state: WriteState,
    has_oids: bool,
    value_writer: W,
    buf: Vec<u8>,
}

impl<W> fmt::Debug for BinaryCopyWriter<W>
    where W: fmt::Debug
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("BinaryCopyWriter")
           .field("state", &self.state)
           .field("has_oids", &self.has_oids)
           .field("value_writer", &self.value_writer)
           .field("buf", &self.buf.len())
           .finish()
    }
}

impl<W> BinaryCopyWriter<W>
    where W: WriteValue
{
    /// Creates a new `BinaryCopyWriter`.
    ///
    /// The writer will forward SQL values to the specified `WriteValue`.
    pub fn new(value_writer: W) -> BinaryCopyWriter<W> {
        BinaryCopyWriter {
            state: WriteState::AtHeader,
            has_oids: false,
            value_writer: value_writer,
            buf: Vec::new(),
        }
    }

    fn read_to(&mut self, buf: &[u8], size: usize) -> io::Result<(bool, usize)> {
        let to_read = cmp::min(size - self.buf.len(), buf.len());
        let nread = try!(self.buf.write(&buf[..to_read]));
        Ok((nread == to_read, nread))
    }

    fn read_header(&mut self, buf: &[u8]) -> io::Result<usize> {
        let header_size = HEADER_MAGIC.len() + mem::size_of::<i32>() * 2;
        let (done, nread) = try!(self.read_to(buf, header_size));
        if !done {
            return Ok(nread);
        }

        if &self.buf[..HEADER_MAGIC.len()] != HEADER_MAGIC {
            let err: Box<error::Error + Sync + Send> = "Did not receive expected header".into();
            return Err(io::Error::new(io::ErrorKind::InvalidInput, err));
        }

        let flags = try!((&mut &self.buf[HEADER_MAGIC.len()..]).read_i32::<BigEndian>());

        self.has_oids = (flags & 1 << 16) != 0;

        if (flags & !0 << 17) != 0 {
            let err: Box<error::Error + Sync + Send> = "Critical file format issue".into();
            return Err(io::Error::new(io::ErrorKind::InvalidInput, err));
        }

        self.buf.clear();
        self.state = WriteState::AtTuple;
        Ok(nread)
    }

    fn read_tuple(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (done, nread) = try!(self.read_to(buf, mem::size_of::<i16>()));
        if !done {
            return Ok(nread);
        }

        let mut tuple_size = try!((&mut &self.buf[..]).read_i16::<BigEndian>());

        self.buf.clear();
        if tuple_size == -1 {
            self.state = WriteState::Done;
            Ok(nread)
        } else {
            if self.has_oids {
                tuple_size += 1;
            }
            self.state = WriteState::AtFieldSize(tuple_size as usize);
            Ok(nread)
        }
    }

    fn read_field_size(&mut self,
                       buf: &[u8],
                       info: &CopyInfo,
                       remaining: usize)
                       -> io::Result<usize> {
        let (done, nread) = try!(self.read_to(buf, mem::size_of::<i32>()));
        if !done {
            return Ok(nread);
        }

        let field_size = try!((&mut &self.buf[..]).read_i32::<BigEndian>());

        self.buf.clear();
        if field_size == -1 {
            try!(self.value_writer.write_null_value(info));
            self.advance_field_state(remaining);
        } else {
            self.state = WriteState::AtField {
                size: field_size as usize,
                remaining: remaining,
            };
        }
        Ok(nread)
    }

    fn advance_field_state(&mut self, remaining: usize) {
        self.state = if remaining == 1 {
            WriteState::AtTuple
        } else {
            WriteState::AtFieldSize(remaining - 1)
        };
    }

    fn read_field(&mut self,
                  buf: &[u8],
                  info: &CopyInfo,
                  size: usize,
                  remaining: usize)
                  -> io::Result<usize> {
        let (done, nread) = try!(self.read_to(buf, size));
        if !done {
            return Ok(nread);
        }

        try!(self.value_writer.write_value(&mut WriteValueReader(&mut &self.buf[..]), info));
        self.buf.clear();
        self.advance_field_state(remaining);
        Ok(nread)
    }
}

impl<W> WriteWithInfo for BinaryCopyWriter<W>
    where W: WriteValue
{
    fn write_with_info(&mut self, buf: &[u8], info: &CopyInfo) -> io::Result<usize> {
        match self.state {
            WriteState::AtHeader => self.read_header(buf),
            WriteState::AtTuple => self.read_tuple(buf),
            WriteState::AtFieldSize(remaining) => self.read_field_size(buf, info, remaining),
            WriteState::AtField { size, remaining } => self.read_field(buf, info, size, remaining),
            WriteState::Done => {
                let err: Box<error::Error + Sync + Send> = "Unexpected input after stream end"
                                                               .into();
                Err(io::Error::new(io::ErrorKind::InvalidInput, err))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use postgres::{Connection, SslMode};
    use postgres::types::{Type, FromSql, ToSql};
    use postgres::stmt::CopyInfo;

    #[test]
    fn write_basic() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY, bar VARCHAR)",
                     &[])
            .unwrap();

        let stmt = conn.prepare("COPY foo (id, bar) FROM STDIN BINARY").unwrap();

        let types = &[Type::Int4, Type::Varchar];
        let values: Vec<Box<ToSql>> = vec![Box::new(1i32),
                                           Box::new("foobar"),
                                           Box::new(2i32),
                                           Box::new(None::<String>)];
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
    fn write_many_rows() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY, bar VARCHAR)",
                     &[])
            .unwrap();

        let stmt = conn.prepare("COPY foo (id, bar) FROM STDIN BINARY").unwrap();

        let types = &[Type::Int4, Type::Varchar];
        let mut values: Vec<Box<ToSql>> = vec![];
        for i in 0..10_000i32 {
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
    fn write_big_rows() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY, bar BYTEA)",
                     &[])
            .unwrap();

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

    #[test]
    fn read_basic() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        conn.execute("CREATE TEMPORARY TABLE foo (id SERIAL PRIMARY KEY, bar INT)",
                     &[])
            .unwrap();
        conn.execute("INSERT INTO foo (bar) VALUES (1), (2), (NULL), (4)", &[]).unwrap();

        let mut out = vec![];

        {
            let writer = |r: Option<&mut WriteValueReader>, info: &CopyInfo| {
                match r {
                    Some(r) => {
                        out.push(Option::<i32>::from_sql(&Type::Int4, r, &info.session_info())
                                     .unwrap())
                    }
                    None => {
                        out.push(Option::<i32>::from_sql_null(&Type::Int4, &info.session_info())
                                     .unwrap())
                    }
                }
                Ok(())
            };

            let mut writer = BinaryCopyWriter::new(writer);

            let stmt = conn.prepare("COPY (SELECT bar FROM foo ORDER BY id) TO STDOUT BINARY")
                           .unwrap();
            stmt.copy_out(&[], &mut writer).unwrap();
        }

        assert_eq!(out, [Some(1), Some(2), None, Some(4)]);
    }

    #[test]
    fn read_many_rows() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        conn.execute("CREATE TEMPORARY TABLE foo (id INT)", &[]).unwrap();

        let mut expected = vec![];
        let stmt = conn.prepare("INSERT INTO foo (id) VALUES ($1)").unwrap();
        for i in 0..10_000i32 {
            stmt.execute(&[&i]).unwrap();
            expected.push(i);
        }

        let mut out = vec![];

        {
            let writer = |r: Option<&mut WriteValueReader>, info: &CopyInfo| {
                out.push(i32::from_sql(&Type::Int4, r.unwrap(), &info.session_info()).unwrap());
                Ok(())
            };

            let mut writer = BinaryCopyWriter::new(writer);

            let stmt = conn.prepare("COPY (SELECT id FROM foo ORDER BY id) TO STDOUT BINARY")
                           .unwrap();
            stmt.copy_out(&[], &mut writer).unwrap();
        }

        assert_eq!(out, expected);
    }

    #[test]
    fn read_big_rows() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY, bar BYTEA)",
                     &[])
            .unwrap();

        let mut expected = vec![];
        let stmt = conn.prepare("INSERT INTO foo (id, bar) VALUES ($1, $2)").unwrap();
        for i in 0..2i32 {
            let value = vec![i as u8; 128 * 1024];
            stmt.execute(&[&i, &value]).unwrap();
            expected.push(value);
        }

        let mut out = vec![];

        {
            let writer = |r: Option<&mut WriteValueReader>, info: &CopyInfo| {
                out.push(Vec::<u8>::from_sql(&Type::Bytea, r.unwrap(), &info.session_info())
                             .unwrap());
                Ok(())
            };

            let mut writer = BinaryCopyWriter::new(writer);

            let stmt = conn.prepare("COPY (SELECT bar FROM foo ORDER BY id) TO STDOUT (FORMAT \
                                     binary)")
                           .unwrap();
            stmt.copy_out(&[], &mut writer).unwrap();
        }

        assert_eq!(out, expected);
    }

    #[test]
    fn read_with_oids() {
        let conn = Connection::connect("postgres://postgres@localhost", SslMode::None).unwrap();
        conn.execute("CREATE TEMPORARY TABLE foo (id INT) WITH OIDS", &[]).unwrap();
        conn.execute("INSERT INTO foo (id) VALUES (1), (2), (3), (4)", &[]).unwrap();

        let mut oids = vec![];
        let mut out = vec![];

        {
            let writer = |r: Option<&mut WriteValueReader>, info: &CopyInfo| {
                if oids.len() > out.len() {
                    out.push(i32::from_sql(&Type::Bytea, r.unwrap(), &info.session_info())
                                 .unwrap());
                } else {
                    oids.push(u32::from_sql(&Type::Oid, r.unwrap(), &info.session_info()).unwrap());
                }
                Ok(())
            };

            let mut writer = BinaryCopyWriter::new(writer);

            let stmt = conn.prepare("COPY foo (id) TO STDOUT (FORMAT binary, OIDS)").unwrap();
            stmt.copy_out(&[], &mut writer).unwrap();
        }

        assert_eq!(oids.len(), out.len());
        assert_eq!(out, [1, 2, 3, 4]);
    }
}
