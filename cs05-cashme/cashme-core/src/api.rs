/*
 * the API of the library -- structs that the server accepts or returns
 *
 * there are 6 basic commands:
 * AllocateStart
 * AllocateFinish
 * InquireStatus
 * StartReading
 * FinishReading
 * Deallocate
 *
 * each of these accept a key param. In case of Allocate, there is additionally length param for
 * how much to allocate, and in case of FinishReading there is reader_id, whose value is to be
 * taken from StartReading's response
 *
 * TODO to self -- do we support metadata like deser_fun or data_type?
 * TODO to self -- deterministic translation of arbitrary keys into Key on the client side? But
 * probably want to expose in the core here. Then the keys can correspond directly to shmids --
 * again a function exposed here
 *
 * then there are 2 server commands
 * ServerShutdown
 * ServerStatus
 *
 * with no params
 */


use rkyv::{Archive, Serialize, Deserialize};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
struct Key (pub [u8; 32]);

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
enum Request {
    AllocateStart { key: Key, bytes_length: u32 },
    AllocateFinish { key: Key },
    InquireStatus { key: Key },
    StartReading { key: Key },
    FinishReading { key: Key, reader_id: u32 },
    Deallocate { key: Key },
    ServerShutdown,
    ServerStatus,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
enum DatasetStatus {
    Missing,
    Preparing,
    Available,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(compare(PartialEq), derive(Debug))]
enum Response {
    StartReading { bytes_length: u32, reader_id: u32 },
    InquireStatus { status: DatasetStatus },
    GenericOk, // all except the two above just get this response or an Error
    Error { detail: String }, // note to self: String here makes it more performant for the other
                              // responses than using eg [u8;128], as the enum size is max over
                              // variant sizes
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::{deserialize, rancor::Error};

    #[test]
    fn test_serialization() {
        let a1 = Request::AllocateStart{
            key: Key([1; 32]),
            bytes_length: 42,
        };
        let a1b = rkyv::to_bytes::<Error>(&a1).unwrap();

        let a1r = rkyv::access::<ArchivedRequest, Error>(&a1b[..]).unwrap();
        assert_eq!(a1r, &a1);

        let a1f = deserialize::<Request, Error>(a1r).unwrap();
        assert_eq!(a1f, a1);
    }

}
