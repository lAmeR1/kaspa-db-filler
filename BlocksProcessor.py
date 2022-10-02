# encoding: utf-8

import asyncio
import logging
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from dbsession import session_maker
from models.Block import Block
from models.Transaction import Transaction, TransactionOutput, TransactionInput
from utils.Event import Event

_logger = logging.getLogger(__name__)

CLUSTER_SIZE_INITIAL = 180 * 20
CLUSTER_SIZE_SYNCED = 5
CLUSTER_WAIT_SECONDS = 5


class BlocksProcessor(object):
    """
    BlocksProcessor polls kaspad for blocks and adds the meta information and it's transactions into database.
    """

    def __init__(self, client):
        self.client = client
        self.blocks_to_add = []
        self.on_commited = Event()

        self.txs = {}
        self.txs_output = []
        self.txs_input = []

        # Did the loop already see the DAG tip
        self.synced = False

    async def loop(self, start_point):
        # go through each block added to DAG
        async for block_hash, block in self.blockiter(start_point):
            # prepare add block and tx to database
            await self.__add_block_to_queue(block_hash, block)
            await self.__add_tx_to_queue(block_hash, block)

            # if cluster size is reached, insert to database
            if len(self.blocks_to_add) >= (CLUSTER_SIZE_INITIAL if not self.synced else CLUSTER_SIZE_SYNCED):
                await self.commit_blocks()
                await self.commit_txs()
                await self.on_commited()

    async def blockiter(self, start_point):
        """
        generator for iterating the blocks added to the blockDAG
        """
        low_hash = start_point
        while True:
            resp = await self.client.request("getBlocksRequest",
                                             params={
                                                 "lowHash": low_hash,
                                                 "includeTransactions": True,
                                                 "includeBlocks": True
                                             },
                                             timeout=60)

            # if it's not synced, get the tiphash, which has to be found for getting synced
            if not self.synced:
                daginfo = await self.client.request("getBlockDagInfoRequest", {})

            # go through each block and yield
            for i, _ in enumerate(resp["getBlocksResponse"].get("blockHashes", [])):

                if not self.synced:
                    if daginfo["getBlockDagInfoResponse"]["tipHashes"][0] == _:
                        _logger.info('Found tip hash. Generator is synced now.')
                        self.synced = True

                # ignore the first block, which is not start point. It is already processed by previous request
                if _ == low_hash and _ != start_point:
                    continue

                # yield blockhash and it's data
                yield _, resp["getBlocksResponse"]["blocks"][i]

            # new low hash is the last hash of previous response
            if len(resp["getBlocksResponse"].get("blockHashes", [])) > 1:
                low_hash = resp["getBlocksResponse"]["blockHashes"][-1]
            else:
                _logger.debug('')
                await asyncio.sleep(2)

            # if synced, poll blocks after 1s
            if self.synced:
                _logger.debug(f'Waiting for the next blocks request. ({len(self.blocks_to_add)}/{CLUSTER_SIZE_SYNCED})')
                await asyncio.sleep(CLUSTER_WAIT_SECONDS)

    async def __add_tx_to_queue(self, block_hash, block):
        """
        Adds block's transactions to queue. This is only prepartion without commit!
        """
        # Go through blocks
        for transaction in block["transactions"]:
            tx_id = transaction["verboseData"]["transactionId"]

            # Check, that the transaction isn't prepared yet. Otherwise ignore
            # Often transactions are added in more than one block
            if not self.is_tx_id_in_queue(transaction["verboseData"]["transactionId"]):
                # Add transaction

                self.txs[tx_id] = Transaction(subnetwork_id=transaction["subnetworkId"],
                                              transaction_id=transaction["verboseData"]["transactionId"],
                                              hash=transaction["verboseData"]["hash"],
                                              mass=transaction["verboseData"].get("mass"),
                                              block_hash=[transaction["verboseData"]["blockHash"]],
                                              block_time=int(transaction["verboseData"]["blockTime"]))

                # Add transactions output
                for index, out in enumerate(transaction["outputs"]):
                    self.txs_output.append(TransactionOutput(transaction_id=transaction["verboseData"]["transactionId"],
                                                             index=index,
                                                             amount=out["amount"],
                                                             script_public_key=out["scriptPublicKey"]["scriptPublicKey"],
                                                             script_public_key_address=out["verboseData"][
                                                                 "scriptPublicKeyAddress"],
                                                             script_public_key_type=out["verboseData"][
                                                                 "scriptPublicKeyType"]))
                # Add transactions input
                for index, tx_in in enumerate(transaction.get("inputs", [])):
                    self.txs_input.append(TransactionInput(transaction_id=transaction["verboseData"]["transactionId"],
                                                           index=index,
                                                           previous_outpoint_hash=tx_in["previousOutpoint"][
                                                               "transactionId"],
                                                           previous_outpoint_index=tx_in["previousOutpoint"].get(
                                                               "index", 0),
                                                           signature_script=tx_in["signatureScript"],
                                                           sig_op_count=tx_in["sigOpCount"]))
            else:
                # If the block if already in the Queue, merge the block_hashes.
                self.txs[tx_id].block_hash = list(set(self.txs[tx_id].block_hash + [block_hash]))

    async def commit_txs(self):
        """
        Add all queued transactions and it's in- and outputs to database
        """
        # First go through all transactions and check, if there are already added ones.
        # If yes, update block_hash and remove from queue
        tx_ids_to_add = list(self.txs.keys())
        with session_maker() as session:
            tx_items = session.query(Transaction).filter(Transaction.transaction_id.in_(tx_ids_to_add)).all()
            for tx_item in tx_items:
                tx_item.block_hash = list((set(tx_item.block_hash) | set(self.txs[tx_item.transaction_id].block_hash)))
                self.txs.pop(tx_item.transaction_id)

            session.commit()

        # Go through all transactions which were not in the database and add now.
        with session_maker() as session:
            # go through queues and add
            for _ in self.txs.values():
                session.add(_)

            for tx_output in self.txs_output:
                if tx_output.transaction_id in self.txs:
                    session.add(tx_output)

            for tx_input in self.txs_input:
                if tx_input.transaction_id in self.txs:
                    session.add(tx_input)

            try:
                session.commit()
                _logger.debug(f'Added {len(self.txs)} TXs to database')

                # reset queues
                self.txs = {}
                self.txs_input = []
                self.txs_output = []

            except IntegrityError:
                session.rollback()
                _logger.error(f'Error adding TXs to database')
                raise

    async def __add_block_to_queue(self, block_hash, block):
        """
        Adds a block to the queue, which is used for adding a cluster
        """

        block_entity = Block(hash=block_hash,
                             accepted_id_merkle_root=block["header"]["acceptedIdMerkleRoot"],
                             difficulty=block["verboseData"]["difficulty"],
                             is_chain_block=block["verboseData"].get("isChainBlock", False),
                             merge_set_blues_hashes=block["verboseData"].get("mergeSetBluesHashes", []),
                             merge_set_reds_hashes=block["verboseData"].get("mergeSetRedsHashes", []),
                             selected_parent_hash=block["verboseData"]["selectedParentHash"],
                             bits=block["header"]["bits"],
                             blue_score=int(block["header"]["blueScore"]),
                             blue_work=block["header"]["blueWork"],
                             daa_score=int(block["header"]["daaScore"]),
                             hash_merkle_root=block["header"]["hashMerkleRoot"],
                             nonce=block["header"]["nonce"],
                             parents=block["header"]["parents"][0]["parentHashes"],
                             pruning_point=block["header"]["pruningPoint"],
                             timestamp=datetime.fromtimestamp(int(block["header"]["timestamp"]) / 1000).isoformat(),
                             utxo_commitment=block["header"]["utxoCommitment"],
                             version=block["header"]["version"])

        # remove same block hash
        self.blocks_to_add = [b for b in self.blocks_to_add if b.hash != block_hash]
        self.blocks_to_add.append(block_entity)

    async def commit_blocks(self):
        """
        Insert queued blocks to database
        """
        # delete already set old blocks
        with session_maker() as session:
            d = session.query(Block).filter(
                Block.hash.in_([b.hash for b in self.blocks_to_add])).delete()
            session.commit()

        # insert blocks
        with session_maker() as session:
            for _ in self.blocks_to_add:
                session.add(_)
            try:
                session.commit()
                _logger.debug(f'Added {len(self.blocks_to_add)} blocks to database. '
                              f'Timestamp: {self.blocks_to_add[-1].timestamp}')

                # reset queue
                self.blocks_to_add = []
            except IntegrityError:
                session.rollback()
                _logger.error('Error adding group of blocks')
                raise

    def is_tx_id_in_queue(self, tx_id):
        """
        Checks if given TX ID is already in the queue
        """
        return tx_id in self.txs
