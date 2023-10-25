#[cfg(feature = "server")]
mod server;

#[cfg(feature = "client")]
mod client;

fn main() {
    #[cfg(feature = "server")]
    {
        server::run_server();
    }

    #[cfg(feature = "client")]
    {
        client::run_client();
    }

    #[cfg(not(any(feature = "client", feature = "server")))]
    {
        println!("Please specify a feature: client or server.");
    }
}
