#[derive(rustee_macros::FromHeader)]
#[rustee(header = "not a header")]
struct RequestId(u64);

fn main() {}
