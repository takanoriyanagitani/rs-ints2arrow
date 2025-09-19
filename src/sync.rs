#[cfg(feature = "fs")]
pub mod fs;

use std::io;

use io::Read;

use arrow::datatypes::ArrowPrimitiveType;

use arrow::datatypes::Int8Type;
use arrow::datatypes::Int16Type;
use arrow::datatypes::Int32Type;
use arrow::datatypes::Int64Type;

use arrow::datatypes::UInt8Type;
use arrow::datatypes::UInt16Type;
use arrow::datatypes::UInt32Type;
use arrow::datatypes::UInt64Type;

use arrow::array::PrimitiveArray;
use arrow::array::PrimitiveBuilder;

macro_rules! ints2arrow {
    ($fname: ident, $ity: ty) => {
        /// Converts the iterator of integers to an array of integers.
        pub fn $fname<I>(ints: I) -> Result<PrimitiveArray<$ity>, io::Error>
        where
            I: Iterator<Item = Result<<$ity as ArrowPrimitiveType>::Native, io::Error>>,
        {
            let mut bldr = PrimitiveBuilder::new();
            for ri in ints {
                let i = ri?;
                bldr.append_value(i);
            }
            Ok(bldr.finish())
        }
    };
}

ints2arrow!(ints2arrow8, Int8Type);
ints2arrow!(ints2arrow16, Int16Type);
ints2arrow!(ints2arrow32, Int32Type);
ints2arrow!(ints2arrow64, Int64Type);

ints2arrow!(uints2arrow8, UInt8Type);
ints2arrow!(uints2arrow16, UInt16Type);
ints2arrow!(uints2arrow32, UInt32Type);
ints2arrow!(uints2arrow64, UInt64Type);

pub fn raw2ints_rtfn<R, T, F, const N: usize>(
    mut rdr: R,
    buf2t: F,
) -> impl Iterator<Item = Result<T, io::Error>>
where
    R: Read,
    F: Fn([u8; N]) -> T,
{
    let mut buf: [u8; N] = [0; N];
    std::iter::from_fn(move || {
        let rslt = rdr.read_exact(&mut buf);
        match rslt {
            Ok(_) => Some(Ok(buf2t(buf))),
            Err(e) => match e.kind() {
                io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(e)),
            },
        }
    })
}

macro_rules! raw2ints {
    ($fname: ident, $ity: ty, $buf2i: expr, $bufsz: literal) => {
        /// Converts the reader to an iterator of integers.
        pub fn $fname<R>(rdr: R) -> impl Iterator<Item = Result<$ity, io::Error>>
        where
            R: Read,
        {
            raw2ints_rtfn(rdr, $buf2i)
        }
    };
}

raw2ints!(raw2ints8, i8, |buf: [u8; 1]| buf[0] as i8, 1);
raw2ints!(raw2uints8, u8, |buf: [u8; 1]| buf[0], 1);

raw2ints!(raw2ints16le, i16, i16::from_le_bytes, 2);
raw2ints!(raw2ints32le, i32, i32::from_le_bytes, 4);
raw2ints!(raw2ints64le, i64, i64::from_le_bytes, 8);

raw2ints!(raw2uints16le, u16, u16::from_le_bytes, 2);
raw2ints!(raw2uints32le, u32, u32::from_le_bytes, 4);
raw2ints!(raw2uints64le, u64, u64::from_le_bytes, 8);

raw2ints!(raw2ints16be, i16, i16::from_be_bytes, 2);
raw2ints!(raw2ints32be, i32, i32::from_be_bytes, 4);
raw2ints!(raw2ints64be, i64, i64::from_be_bytes, 8);

raw2ints!(raw2uints16be, u16, u16::from_be_bytes, 2);
raw2ints!(raw2uints32be, u32, u32::from_be_bytes, 4);
raw2ints!(raw2uints64be, u64, u64::from_be_bytes, 8);

macro_rules! raw2ints2arrow {
    ($fname: ident, $rdr2i: ident, $i2arr: ident, $aty: ty) => {
        /// Converts the reader to an array of integers.
        pub fn $fname<R>(rdr: R) -> Result<PrimitiveArray<$aty>, io::Error>
        where
            R: Read,
        {
            let i = $rdr2i(rdr);
            $i2arr(i)
        }
    };
}

raw2ints2arrow!(raw2ints2arrow8, raw2ints8, ints2arrow8, Int8Type);
raw2ints2arrow!(raw2uints2arrow8, raw2uints8, uints2arrow8, UInt8Type);

raw2ints2arrow!(raw2ints2arrow16le, raw2ints16le, ints2arrow16, Int16Type);
raw2ints2arrow!(raw2ints2arrow32le, raw2ints32le, ints2arrow32, Int32Type);
raw2ints2arrow!(raw2ints2arrow64le, raw2ints64le, ints2arrow64, Int64Type);
raw2ints2arrow!(raw2ints2arrow16be, raw2ints16be, ints2arrow16, Int16Type);
raw2ints2arrow!(raw2ints2arrow32be, raw2ints32be, ints2arrow32, Int32Type);
raw2ints2arrow!(raw2ints2arrow64be, raw2ints64be, ints2arrow64, Int64Type);

raw2ints2arrow!(
    raw2uints2arrow16le,
    raw2uints16le,
    uints2arrow16,
    UInt16Type
);
raw2ints2arrow!(
    raw2uints2arrow32le,
    raw2uints32le,
    uints2arrow32,
    UInt32Type
);
raw2ints2arrow!(
    raw2uints2arrow64le,
    raw2uints64le,
    uints2arrow64,
    UInt64Type
);
raw2ints2arrow!(
    raw2uints2arrow16be,
    raw2uints16be,
    uints2arrow16,
    UInt16Type
);
raw2ints2arrow!(
    raw2uints2arrow32be,
    raw2uints32be,
    uints2arrow32,
    UInt32Type
);
raw2ints2arrow!(
    raw2uints2arrow64be,
    raw2uints64be,
    uints2arrow64,
    UInt64Type
);
