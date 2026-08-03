async fn endpoint() {}

fn main() {
    let _app = rustee::routes!(
        rustee::App::new();
        BREW "/coffee" => endpoint,
    );
}
