# encoding: utf-8
import asyncio
import logging

from dbsession import session_maker
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
        self.virtual_chain_list = []
        self.__prepared_list = []
        self.client = client

    async def loop(self, start_point):
        while True:
            _logger.debug('Polling now.')
            resp = await self.client.request("getVirtualSelectedParentChainFromBlockRequest",
                                             {"startHash": start_point,
                                              "includeAcceptedTransactionIds": True},
                                             timeout=120)

            # if there is a response, add to queue and set new startpoint
            if resp["getVirtualSelectedParentChainFromBlockResponse"]:
                self.virtual_chain_list.append(resp["getVirtualSelectedParentChainFromBlockResponse"])
                start_point = self.virtual_chain_list[-1]["addedChainBlockHashes"][-1]

            # wait before polling
            await asyncio.sleep(POLLING_TIME_IN_S)

    async def __update_transactions(self, parent_chain_response):
        """
        goes through one parentChainResponse and updates the is_accepted field in the database.
        """
        accepted_ids = []
        rejected_blocks = []

        for tx_accept_dict in parent_chain_response['acceptedTransactionIds']:
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

    async def prepare(self):
        """
        Stores the virtual chain list into a temporary storage. This is a preparation for adding.
        """
        self.__prepared_list = self.virtual_chain_list
        self.virtual_chain_list = []

    def is_prepared(self):
        return len(self.__prepared_list) > 0

    async def yield_to_database(self):
        """
        Adds prepared temporary storage to database
        """
        for vchain_entry in self.__prepared_list:
            await self.__update_transactions(vchain_entry)

        # reset temp storage
        self.__prepared_list = []
