# encoding: utf-8
import asyncio
import logging

from dbsession import session_maker
from helper import KeyValueStore
from models.Block import Block
from models.Transaction import Transaction

_logger = logging.getLogger(__name__)


class VirtualChainProcessor(object):
    """
    VirtualChainProcessor polls the command getVirtualSelectedParentChainFromBlockRequest and updates transactions
    with is_accepted False or True.

    To make sure all blocks are already in database, the VirtualChain processor has a prepare function, which is
    basically a temporary storage. This buffer should be processed AFTER the blocks and transactions are added.
    """

    def __init__(self, client, start_point):
        self.virtual_chain_response = None
        self.start_point = start_point
        self.client = client

    async def __update_transactions_in_db(self):
        """
        goes through one parentChainResponse and updates the is_accepted field in the database.
        """
        accepted_ids = []
        rejected_blocks = []
        last_known_chain_block = None

        if self.virtual_chain_response is None:
            return

        parent_chain_response = self.virtual_chain_response
        parent_chain_blocks = [x['acceptingBlockHash'] for x in parent_chain_response['acceptedTransactionIds']]

        # find intersection of database blocks and virtual parent chain
        with session_maker() as s:
            parent_chain_blocks_in_db = s.query(Block) \
                .filter(Block.hash.in_(parent_chain_blocks)) \
                .with_entities(Block.hash).all()
            parent_chain_blocks_in_db = [x[0] for x in parent_chain_blocks_in_db]

        # parent_chain_blocks_in_db = parent_chain_blocks_in_db[:200]

        # go through all acceptedTransactionIds and stop if a block hash is not in database
        for tx_accept_dict in parent_chain_response['acceptedTransactionIds']:
            accepting_block_hash = tx_accept_dict['acceptingBlockHash']

            if accepting_block_hash not in parent_chain_blocks_in_db:
                break  # Stop once we reached a non-existing block

            last_known_chain_block = accepting_block_hash
            accepted_ids.append((tx_accept_dict['acceptingBlockHash'], tx_accept_dict["acceptedTransactionIds"]))

            if len(accepted_ids) >= 5000:
                break

        # add rejected blocks if needed
        rejected_blocks.extend(parent_chain_response.get('removedChainBlockHashes', []))

        with session_maker() as s:
            # set is_accepted to False, when blocks were removed from virtual parent chain
            if rejected_blocks:
                count = s.query(Transaction).filter(Transaction.accepting_block_hash.in_(rejected_blocks)) \
                    .update({'is_accepted': False, 'accepting_block_hash': None})
                _logger.debug(f'Set is_accepted=False for {count} TXs')
                s.commit()

            count_tx = 0

            # set is_accepted to True and add accepting_block_hash
            for accepting_block_hash, accepted_tx_ids in accepted_ids:
                s.query(Transaction).filter(Transaction.transaction_id.in_(accepted_tx_ids)) \
                    .update({'is_accepted': True, 'accepting_block_hash': accepting_block_hash})
                count_tx += len(accepted_tx_ids)

            _logger.debug(f'Set is_accepted=True for {count_tx} transactions.')
            s.commit()

        # Mark last known/processed as start point for the next query
        if last_known_chain_block:
            KeyValueStore.set("vspc_last_start_hash", last_known_chain_block)
            self.start_point = last_known_chain_block

        # Clear the current response
        self.virtual_chain_response = None

    async def yield_to_database(self):
        """
        Add known blocks to database
        """
        resp = await self.client.request("getVirtualSelectedParentChainFromBlockRequest",
                                         {"startHash": self.start_point,
                                          "includeAcceptedTransactionIds": True},
                                         timeout=240)

        # if there is a response, add to queue and set new startpoint
        if resp["getVirtualSelectedParentChainFromBlockResponse"]:
            _logger.debug(f'Got response with '
                          f'{len(resp["getVirtualSelectedParentChainFromBlockResponse"]["addedChainBlockHashes"])}'
                          f' addedChainBlockHashes')
            self.virtual_chain_response = resp["getVirtualSelectedParentChainFromBlockResponse"]
        else:
            _logger.debug('Empty response.')
            self.virtual_chain_response = None

        asyncio.create_task(self.__update_transactions_in_db())
