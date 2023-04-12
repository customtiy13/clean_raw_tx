use log::error;

fn main() {
    env_logger::init();

    if let Err(e) = clean_raw_tx::get_args().and_then(clean_raw_tx::run) {
        error!("{e}");
        std::process::exit(-1);
    }
}
