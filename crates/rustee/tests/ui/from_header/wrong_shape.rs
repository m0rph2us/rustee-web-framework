#[derive(rustee_macros::FromHeader)]
#[rustee(header = "x-request-id")]
struct RequestId {
    value: u64,
}

fn main() {}
