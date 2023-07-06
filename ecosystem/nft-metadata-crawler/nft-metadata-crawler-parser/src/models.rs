// Copyright © Aptos Foundation

use chrono::{NaiveDateTime, Utc};
use diesel::prelude::*;

#[derive(Clone, Insertable, Queryable, Selectable)]
#[diesel(table_name = crate::schema::nft_metadata_crawler_entry)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NFTMetadataCrawlerEntry {
    pub token_data_id: String,
    pub token_uri: String,
    pub retry_count: i32,
    pub last_transaction_version: i32,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub last_updated: chrono::NaiveDateTime,
}

impl NFTMetadataCrawlerEntry {
    pub fn new(s: String) -> Self {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() == 4 {
            return Self {
                token_data_id: parts[0].to_string(),
                token_uri: parts[1].to_string(),
                retry_count: 0,
                last_transaction_version: parts[2]
                    .to_string()
                    .parse()
                    .expect("Error parsing last_transaction_version"),
                last_transaction_timestamp: NaiveDateTime::parse_from_str(
                    parts[3],
                    "%Y-%m-%d %H:%M:%S %Z",
                )
                .expect("Error parsing last_transaction_timestamp"),
                last_updated: Utc::now().naive_utc(),
            };
        } else {
            panic!("Error parsing record");
        }
    }
}

#[derive(Clone, Insertable, Queryable, Selectable)]
#[diesel(table_name = crate::schema::nft_metadata_crawler_uris)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NFTMetadataCrawlerURIs {
    pub token_uri: String,
    pub raw_image_uri: Option<String>,
    pub cdn_json_uri: Option<String>,
    pub cdn_image_uri: Option<String>,
    pub image_resizer_retry_count: i32,
    pub json_parser_retry_count: i32,
    pub last_updated: chrono::NaiveDateTime,
}
