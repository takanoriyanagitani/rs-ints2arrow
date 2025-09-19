use std::io;
use std::path::Path;

use io::BufReader;

use arrow::array::PrimitiveArray;
use arrow::datatypes::ArrowPrimitiveType;

use arrow::datatypes::Int8Type;
use arrow::datatypes::Int16Type;
use arrow::datatypes::Int32Type;
use arrow::datatypes::Int64Type;

use arrow::datatypes::UInt8Type;
use arrow::datatypes::UInt16Type;
use arrow::datatypes::UInt32Type;
use arrow::datatypes::UInt64Type;

pub fn filename2bufrdr<P>(filename: P) -> Result<BufReader<std::fs::File>, io::Error>
where
    P: AsRef<Path>,
{
    let f = std::fs::File::open(filename)?;
    Ok(BufReader::new(f))
}

pub fn filename2array<P, F, T>(filename: P, rdr2arr: F) -> Result<PrimitiveArray<T>, io::Error>
where
    P: AsRef<Path>,
    T: ArrowPrimitiveType,
    F: Fn(BufReader<std::fs::File>) -> Result<PrimitiveArray<T>, io::Error>,
{
    let rdr = filename2bufrdr(filename)?;
    rdr2arr(rdr)
}

macro_rules! filename2ints {
    ($fname: ident, $rdr2arr: ident, $ity: ty) => {
        /// Reads the file and converts it to an array of integers.
        pub fn $fname<P>(filename: P) -> Result<PrimitiveArray<$ity>, io::Error>
        where
            P: AsRef<Path>,
        {
            filename2array(filename, $rdr2arr)
        }
    };
}

use super::raw2ints2arrow8;
use super::raw2uints2arrow8;

use super::raw2ints2arrow16be;
use super::raw2ints2arrow16le;
use super::raw2ints2arrow32be;
use super::raw2ints2arrow32le;
use super::raw2ints2arrow64be;
use super::raw2ints2arrow64le;

use super::raw2uints2arrow16be;
use super::raw2uints2arrow16le;
use super::raw2uints2arrow32be;
use super::raw2uints2arrow32le;
use super::raw2uints2arrow64be;
use super::raw2uints2arrow64le;

filename2ints!(filename2ints8, raw2ints2arrow8, Int8Type);
filename2ints!(filename2uints8, raw2uints2arrow8, UInt8Type);

filename2ints!(filename2ints16le, raw2ints2arrow16le, Int16Type);
filename2ints!(filename2ints32le, raw2ints2arrow32le, Int32Type);
filename2ints!(filename2ints64le, raw2ints2arrow64le, Int64Type);
filename2ints!(filename2ints16be, raw2ints2arrow16be, Int16Type);
filename2ints!(filename2ints32be, raw2ints2arrow32be, Int32Type);
filename2ints!(filename2ints64be, raw2ints2arrow64be, Int64Type);

filename2ints!(filename2uints16le, raw2uints2arrow16le, UInt16Type);
filename2ints!(filename2uints32le, raw2uints2arrow32le, UInt32Type);
filename2ints!(filename2uints64le, raw2uints2arrow64le, UInt64Type);
filename2ints!(filename2uints16be, raw2uints2arrow16be, UInt16Type);
filename2ints!(filename2uints32be, raw2uints2arrow32be, UInt32Type);
filename2ints!(filename2uints64be, raw2uints2arrow64be, UInt64Type);
