// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{publishing::publish_util::Package, TransactionExecutor};
use crate::{
    call_custom_modules::{TransactionGeneratorWorker, UserModuleTransactionGenerator},
    get_account_to_burn_from_pool,
};
use aptos_infallible::RwLock;
use aptos_logger::info;
use aptos_sdk::{
    bcs,
    move_types::ident_str,
    transaction_builder::TransactionFactory,
    types::{
        transaction::{EntryFunction, SignedTransaction, TransactionPayload},
        LocalAccount,
    },
};
use async_trait::async_trait;
use rand::rngs::StdRng;
use std::sync::Arc;

pub struct NbcuV1MintTransactionGenerator {
    pub accounts_pool: Arc<RwLock<Vec<LocalAccount>>>,
}

#[async_trait]
impl UserModuleTransactionGenerator for NbcuV1MintTransactionGenerator {
    fn initialize_package(
        &mut self,
        _package: &Package,
        _publisher: &mut LocalAccount,
        _txn_factory: &TransactionFactory,
        _rng: &mut StdRng,
    ) -> Vec<SignedTransaction> {
        vec![]
    }

    async fn create_generator_fn(
        &self,
        _init_accounts: &mut [LocalAccount],
        _txn_factory: &TransactionFactory,
        _txn_executor: &dyn TransactionExecutor,
        _rng: &mut StdRng,
    ) -> Arc<TransactionGeneratorWorker> {
        let accounts_pool = self.accounts_pool.clone();

        Arc::new(move |fee_payer, package, publisher, txn_factory, _rng| {
            let accounts_to_burn = get_account_to_burn_from_pool(&accounts_pool, 1);
            fee_payer.sign_multi_agent_with_transaction_builder(
                vec![publisher, accounts_to_burn.get(0).unwrap()],
                txn_factory.payload(TransactionPayload::EntryFunction(EntryFunction::new(
                    package.get_module_id("bugs"),
                    ident_str!("mint_token").to_owned(),
                    vec![],
                    vec![],
                ))),
            )
        })
    }
}

pub struct NbcuPremintMintTransactionGenerator {
    pub accounts_pool: Arc<RwLock<Vec<LocalAccount>>>,
}

#[async_trait]
impl UserModuleTransactionGenerator for NbcuPremintMintTransactionGenerator {
    fn initialize_package(
        &mut self,
        package: &Package,
        publisher: &mut LocalAccount,
        txn_factory: &TransactionFactory,
        _rng: &mut StdRng,
    ) -> Vec<SignedTransaction> {
        info!("Preminting for {}", publisher.address());
        let batch_size: u64 = 30;

        (0..(80_000 / batch_size))
            .map(|_| {
                publisher.sign_with_transaction_builder(txn_factory.payload(
                    TransactionPayload::EntryFunction(EntryFunction::new(
                        package.get_module_id("bugs"),
                        ident_str!("fill_parallel_vector").to_owned(),
                        vec![],
                        vec![bcs::to_bytes(&batch_size).unwrap()],
                    )),
                ))
            })
            .collect()
    }

    async fn create_generator_fn(
        &self,
        _init_accounts: &mut [LocalAccount],
        _txn_factory: &TransactionFactory,
        _txn_executor: &dyn TransactionExecutor,
        _rng: &mut StdRng,
    ) -> Arc<TransactionGeneratorWorker> {
        let accounts_pool = self.accounts_pool.clone();

        Arc::new(move |fee_payer, package, publisher, txn_factory, _rng| {
            info!("calling mint_token for {}", publisher.address());
            let accounts_to_burn = get_account_to_burn_from_pool(&accounts_pool, 1);
            fee_payer.sign_multi_agent_with_transaction_builder(
                vec![publisher, accounts_to_burn.get(0).unwrap()],
                txn_factory.payload(TransactionPayload::EntryFunction(EntryFunction::new(
                    package.get_module_id("bugs"),
                    ident_str!("mint_token").to_owned(),
                    vec![],
                    vec![],
                ))),
            )
        })
    }
}