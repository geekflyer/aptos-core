// Copyright © Aptos Foundation

use std::{env, error::Error};

use ::futures::future;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use nft_metadata_crawler_parser::{
    db::upsert_entry, establish_connection_pool, models::NFTMetadataCrawlerEntry, parser::Parser,
};
use nft_metadata_crawler_utils::{consume_from_queue, send_ack};
use reqwest::Client;
use tokio::task::JoinHandle;

async fn process_response(
    res: Vec<String>,
    pool: &Pool<ConnectionManager<PgConnection>>,
) -> Result<Vec<NFTMetadataCrawlerEntry>, Box<dyn Error + Send + Sync>> {
    let mut uris: Vec<NFTMetadataCrawlerEntry> = Vec::new();
    for entry in res {
        uris.push(upsert_entry(
            &mut pool.get()?,
            NFTMetadataCrawlerEntry::new(entry),
        )?);
    }
    Ok(uris)
}

fn spawn_parser(
    uri: NFTMetadataCrawlerEntry,
    pool: &Pool<ConnectionManager<PgConnection>>,
    auth: String,
    subscription_name: String,
    ack: String,
    bucket: String,
) -> JoinHandle<()> {
    match pool.get() {
        Ok(mut conn) => tokio::spawn(async move {
            let mut parser = Parser::new(uri, Some((400, 400)), auth.clone(), bucket);
            match parser.parse(&mut conn).await {
                Ok(()) => {
                    let client = Client::new();
                    match send_ack(&client, &auth, &subscription_name, &ack).await {
                        Ok(_) => {
                            println!("Successfully acked {}", parser.entry.token_uri)
                        },
                        Err(e) => println!("Error acking {}: {}", parser.entry.token_uri, e),
                    }
                },
                Err(e) => println!("Error parsing {}: {}", parser.entry.token_uri, e),
            }
        }),
        Err(_) => todo!(),
    }
}

#[tokio::main]
async fn main() {
    println!("Starting parser");
    let pool = establish_connection_pool();
    let client = Client::new();
    let auth = env::var("AUTH").expect("No AUTH");
    let subscription_name = env::var("SUBSCRIPTION_NAME").expect("No SUBSCRIPTION NAME");
    let bucket = env::var("BUCKET").expect("No BUCKET");

    match consume_from_queue(&client, &auth, &subscription_name).await {
        Ok(r) => {
            let (res, acks): (Vec<String>, Vec<String>) = r.into_iter().unzip();
            match process_response(res, &pool).await {
                Ok(uris) => {
                    let handles: Vec<_> = uris
                        .into_iter()
                        .zip(acks.into_iter())
                        .into_iter()
                        .map(|(uri, ack)| {
                            spawn_parser(
                                uri,
                                &pool,
                                auth.clone(),
                                subscription_name.clone(),
                                ack,
                                bucket.clone(),
                            )
                        })
                        .collect();
                    if let Ok(_) = future::try_join_all(handles).await {
                        println!("SUCCESS");
                    }
                },
                Err(e) => println!("Error processing response: {}", e),
            };
        },
        Err(e) => println!("Error consuming from queue: {}", e),
    }
}
