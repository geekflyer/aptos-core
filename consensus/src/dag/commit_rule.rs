// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    dag::{anchor_election::AnchorElection, dag_store::Dag, types::NodeMetadata, CertifiedNode},
    experimental::buffer_manager::OrderedBlocks,
};
use aptos_consensus_types::common::Round;
use aptos_crypto::HashValue;
use aptos_infallible::RwLock;
use aptos_types::{epoch_state::EpochState, ledger_info::LedgerInfoWithSignatures};
use futures_channel::mpsc::UnboundedSender;
use std::sync::Arc;

pub struct CommitRule {
    epoch_state: Arc<EpochState>,
    ordered_block_id: HashValue,
    lowest_unordered_round: Round,
    dag: Arc<RwLock<Dag>>,
    anchor_election: Box<dyn AnchorElection>,
    commit_sender: UnboundedSender<OrderedBlocks>,
}

impl CommitRule {
    pub fn new(
        epoch_state: Arc<EpochState>,
        latest_ledger_info: LedgerInfoWithSignatures,
        dag: Arc<RwLock<Dag>>,
        anchor_election: Box<dyn AnchorElection>,
        commit_sender: UnboundedSender<OrderedBlocks>,
    ) -> Self {
        // TODO: we need to initialize the anchor election based on the dag
        Self {
            epoch_state,
            ordered_block_id: latest_ledger_info.commit_info().id(),
            lowest_unordered_round: latest_ledger_info.commit_info().round() + 1,
            dag,
            anchor_election,
            commit_sender,
        }
    }

    pub fn new_node(&mut self, node: &CertifiedNode) {
        let round = node.round();
        while self.lowest_unordered_round <= round {
            if let Some(direct_anchor) = self.find_first_anchor_with_enough_votes(round) {
                let commit_anchor = self.find_first_anchor_to_commit(direct_anchor);
                self.finalize_order(commit_anchor);
            } else {
                break;
            }
        }
    }

    pub fn find_first_anchor_with_enough_votes(
        &self,
        target_round: Round,
    ) -> Option<Arc<CertifiedNode>> {
        let dag_reader = self.dag.read();
        let mut current_round = self.lowest_unordered_round;
        while current_round < target_round {
            let anchor_author = self.anchor_election.get_anchor(current_round);
            // I "think" it's impossible to get ordered/committed node here but to double check
            if let Some(anchor_node) =
                dag_reader.get_node_by_round_author(current_round, &anchor_author)
            {
                // f+1 or 2f+1?
                if dag_reader
                    .check_votes_for_node(anchor_node.metadata(), &self.epoch_state.verifier)
                {
                    return Some(anchor_node.clone());
                }
            }
            current_round += 2;
        }
        None
    }

    pub fn find_first_anchor_to_commit(
        &self,
        mut current_anchor: Arc<CertifiedNode>,
    ) -> Arc<CertifiedNode> {
        let dag_reader = self.dag.read();
        let anchor_round = current_anchor.round();
        let is_anchor = |metadata: &NodeMetadata| -> bool {
            (metadata.round() ^ anchor_round) & 1 == 0
                && *metadata.author() == self.anchor_election.get_anchor(metadata.round())
        };
        while let Some(next_anchor) = dag_reader
            .reachable(&current_anchor, Some(self.lowest_unordered_round))
            .map(|node_status| node_status.as_node())
            .find(|node| is_anchor(node.metadata()))
        {
            current_anchor = next_anchor.clone();
        }
        current_anchor.clone()
    }

    pub fn finalize_order(&mut self, anchor: Arc<CertifiedNode>) {
        let _failed_anchors: Vec<_> = (self.lowest_unordered_round..anchor.round())
            .step_by(2)
            .map(|failed_round| self.anchor_election.get_anchor(failed_round))
            .collect();
        self.lowest_unordered_round = anchor.round() + 1;

        let mut dag_writer = self.dag.write();
        let _commit_nodes: Vec<_> = dag_writer
            .reachable_mut(&anchor, None)
            .map(|node_status| {
                node_status.mark_as_ordered();
                node_status.as_node()
            })
            .collect();
    }
}
