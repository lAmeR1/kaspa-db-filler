# encoding: utf-8
import asyncio
import logging

from dbsession import session_maker
from models.Block import Block
from models.Transaction import Transaction

_logger = logging.getLogger(__name__)

POLLING_TIME_IN_S = 3


class VirtualChainProcessor(object):
    """
    VirtualChainProcessor polls the command getVirtualSelectedParentChainFromBlockRequest and updates transactions
    with is_accepted False or True.

    To make sure all blocks are already in database, the VirtualChain processor has a prepare function, which is
    basically a temporary storage. This buffer should be processed AFTER the blocks and transactions are added.
    """

    def __init__(self, client):
        self.virtual_chain_response = None
        self.start_point = None
        self.client = client

    async def loop(self, start_point):
        self.start_point = start_point
        while True:
            if self.virtual_chain_response is None:  # Re-query only if previous response was already processed
                _logger.debug('Polling now.')
                resp = await self.client.request("getVirtualSelectedParentChainFromBlockRequest",
                                                 {"startHash": self.start_point,
                                                  "includeAcceptedTransactionIds": True},
                                                 timeout=120)

                # if there is a response, add to queue and set new startpoint
                if resp["getVirtualSelectedParentChainFromBlockResponse"]:
                    self.virtual_chain_response = resp["getVirtualSelectedParentChainFromBlockResponse"]

            # wait before polling
            await asyncio.sleep(POLLING_TIME_IN_S)

    def __is_in_db_mock(self, block_hash):
        # Just a placeholder for DB query logic
        return True

    async def __update_transactions(self):
        """
        goes through one parentChainResponse and updates the is_accepted field in the database.
        """

        if self.virtual_chain_response is None:
            return

        parent_chain_response = self.virtual_chain_response
        # first_chain_block = parent_chain_response['acceptedTransactionIds'][0]['acceptingBlockHash']

        parent_chain_blocks = [x['acceptingBlockHash'] for x in parent_chain_response['acceptedTransactionIds']]

        # find intersection of database blocks and virtual parent chain
        with session_maker() as s:
            parent_chain_blocks_in_db = s.query(Block) \
                .filter(Block.hash.in_(parent_chain_blocks)) \
                .with_entity(Block.hash).all()

        accepted_ids = []
        rejected_blocks = []

        # last_known_chain_block = first_chain_block
        for tx_accept_dict in parent_chain_response['acceptedTransactionIds']:
            accepting_block_hash = tx_accept_dict['acceptingBlockHash']

            if accepting_block_hash not in parent_chain_blocks_in_db:
                break  # Stop once we reached a none existing block

            last_known_chain_block = accepting_block_hash
            accepted_ids.extend(tx_accept_dict["acceptedTransactionIds"])

        # add rejected blocks if needed
        rejected_blocks.extend(parent_chain_response.get('removedChainBlockHashes', []))

        with session_maker() as s:
            # set is_accepted to False, when blocks were removed from virtual parent chain
            if rejected_blocks:
                count = s.query(Transaction).filter(Transaction.block_hash.in_(rejected_blocks)) \
                    .update({'is_accepted': False})
                _logger.debug(f'Set is_accepted=False for {count} TXs')

            # remove double ids
            accepted_ids = list(set(accepted_ids))
            count = s.query(Transaction).filter(Transaction.transaction_id.in_(accepted_ids)).update(
                {'is_accepted': True})
            _logger.debug(f'Set is_accepted=True for {count} TXs')
            s.commit()

            if count != len(accepted_ids):
                _logger.error(f'Unable to set is_accepted field for TXs. '
                              f'Found only {count}/{len(accepted_ids)} TXs in the database')

                _logger.error(set(accepted_ids) - set([x.transaction_id for x in s.query(Transaction).filter(
                    Transaction.transaction_id.in_(accepted_ids)).all()]))

                raise Exception("IsAccepted update not possible")

        # Mark last known/processed as start point for the next query
        self.start_point = last_known_chain_block
        # Clear the current response
        self.virtual_chain_response = None


    async def yield_to_database(self):
        """
        Add known blocks to database
        """
        await self.__update_transactions()
