import asyncio
import logging
import os

from BlocksProcessor import BlocksProcessor
from VirtualChainProcessor import VirtualChainProcessor
from dbsession import create_all, session_maker
from kaspad.KaspadMultiClient import KaspadMultiClient
from models.Transaction import Transaction

logging.basicConfig(format="%(asctime)s::%(levelname)s::%(name)s::%(message)s",
                    level=logging.DEBUG if os.getenv("DEBUG", False) else logging.INFO,
                    handlers=[
                        logging.StreamHandler()
                    ]
                    )

# disable sqlalchemy notifications
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

# get file logger
_logger = logging.getLogger(__name__)

# create tables in database
create_all(drop=False)

kaspad_hosts = []

for i in range(100):
    try:
        kaspad_hosts.append(os.environ[f"KASPAD_HOST_{i + 1}"].strip())
    except KeyError:
        break

if not kaspad_hosts:
    raise Exception('Please set at least KASPAD_HOST_1 environment variable.')

# create Kaspad client
client = KaspadMultiClient(kaspad_hosts)


async def main():
    # initialize kaspads
    await client.initialize_all()

    # find last acceptedTx's block hash, when restarting this tool
    with session_maker() as s:
        try:
            start_hash = s.query(Transaction) \
                .where(Transaction.is_accepted == True) \
                .order_by(Transaction.block_time.desc()) \
                .limit(1) \
                .first() \
                .block_hash
        except AttributeError:
            start_hash = None

    # if there is nothing in the db, just get latest block.
    if not start_hash:
        daginfo = await client.request("getBlockDagInfoRequest", {})
        start_hash = daginfo["getBlockDagInfoResponse"]["tipHashes"][0]

    _logger.info(f"Start hash: {start_hash}")

    # create instances of blocksprocessor and virtualchainprocessor
    bp = BlocksProcessor(client, start_hash)
    vcp = VirtualChainProcessor(client, start_hash)

    async def handle_blocks_commited(e):
        """
        this function is executed, when a new cluster of blocks were added to the database
        """
        if not bp.synced:
            return

        # if there are prepared acceptedTransactions, go through and update database
        if vcp.is_prepared():
            _logger.debug('Update is_accepted for TXs.')
            await vcp.yield_to_database()

        # first, prepare virtual chain to be added soon.
        # this mechanism is needed to guarantee, all TXs are added in database,
        # when going through "acceptedTransactions"
        _logger.debug('acceptedTransactions are prepared to be insterted after adding next blocks.')
        await vcp.prepare()

    # set up event to fire after adding new blocks
    bp.on_commited += handle_blocks_commited

    # start blocks processor adding blocks into database
    await bp.loop()


if __name__ == '__main__':
    asyncio.run(main())
