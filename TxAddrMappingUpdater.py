# encoding: utf-8

import logging
import time
from datetime import datetime, timedelta

from dbsession import session_maker
from models.TxAddrMapping import TxAddrMapping

LIMIT = 2000
PRECONDITION_RETRIES = 2

_logger = logging.getLogger(__name__)


class TxAddrMappingUpdater(object):
    def __init__(self):
        self.last_block_time = None

    def precondition(self):
        for i in range(PRECONDITION_RETRIES):
            with session_maker() as s:
                res = s.execute("SELECT block_time "
                                "FROM tx_id_address_mapping "
                                "ORDER by id "
                                "DESC "
                                "LIMIT 1").first()

                # get last added block time and substract 1 hour just for instance
                if res:
                    self.last_block_time = res[0] - 1000 * 60 * 60
                    break
                else:
                    _logger.info('No block time found in database. Fetching from blocks')
                    res = s.execute("SELECT timestamp FROM blocks ORDER by blue_score ASC LIMIT 1").first()
                    if res:
                        self.last_block_time = int(res[0].timestamp() * 1000)
                        break

            _logger.info(f"Retry {i + 1}. Wait 10s")
            time.sleep(10)

    @staticmethod
    def minimum_timestamp():
        return round((datetime.now() - timedelta(minutes=1)).timestamp() * 1000)

    def loop(self):
        self.precondition()

        error_cnt = 0

        _logger.debug('Start TxAddrMappingUpdater')  # type: TxAddrMapping

        while True:
            try:
                count_outputs, new_last_block_time_outputs = self.update_outputs(self.last_block_time)
                count_inputs, new_last_block_time_inputs = self.update_inputs(self.last_block_time)
            except Exception:
                error_cnt += 1
                if error_cnt <= 3:
                    time.sleep(10)
                    continue
                raise

            _logger.info(f"Updated {count_inputs} input mappings.")
            _logger.info(f"Updated {count_outputs} outputs mappings.")

            # initialize with latest known block time
            new_last_block_time = max(new_last_block_time_outputs or 0,
                                      new_last_block_time_inputs or 0)

            # fallback if nothing added
            if not new_last_block_time_outputs and not new_last_block_time_inputs:
                _logger.debug("No mapping to be added.")
                new_last_block_time = self.get_last_block_time(self.last_block_time)

            # get minimum timestamp to check next loop
            # -> block_time is not an consistent increasing value -> need to leave space in the past
            # -> now() - 1 min
            min_timestamp = TxAddrMappingUpdater.minimum_timestamp()

            # new last block times are sorted in the database!
            next_block_time = min(new_last_block_time_outputs or new_last_block_time,
                                       min_timestamp,
                                       new_last_block_time_inputs or new_last_block_time)

            if self.last_block_time == next_block_time:
                # this only happens, when kaspad is not synced and the last block ( far in the past ) is not changing
                _logger.info('DB is not filled with the current blocks. Wait.')
                time.sleep(10)

            self.last_block_time = next_block_time

            _logger.debug(f"Added TxAddrMapping up to: "
                          f"{datetime.fromtimestamp(self.last_block_time / 1000).isoformat()}")

            if self.last_block_time == min_timestamp:
                time.sleep(10)

    def get_last_block_time(self, start_block_time):
        with session_maker() as s:
            result = s.execute(f"""SELECT
                transactions.block_time
                
                FROM transactions
                WHERE transactions.block_time >= :blocktime
                 ORDER by transactions.block_time ASC
                 LIMIT {LIMIT}""", {"blocktime": start_block_time}).all()

        try:
            return result[-1][0]
        except TypeError:
            return start_block_time

    def update_inputs(self, start_block_time: int):
        with session_maker() as s:
            result = s.execute(f"""INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)

        (SELECT DISTINCT * FROM 
        (SELECT tid, transactions_outputs.script_public_key_address, sq.block_time FROM 
        (SELECT 
        transactions.transaction_id as tid,
        transactions.block_time

        FROM transactions
         WHERE transactions.block_time >= :blocktime) as sq

        LEFT JOIN transactions_inputs ON transactions_inputs.transaction_id = sq.tid
        LEFT JOIN transactions_outputs ON transactions_outputs.transaction_id = transactions_inputs.previous_outpoint_hash AND transactions_outputs.index = transactions_inputs.previous_outpoint_index
         ORDER by sq.block_time ASC
         LIMIT {LIMIT}) as masterq
         WHERE script_public_key_address IS NOT NULL)

         ON CONFLICT DO NOTHING
         RETURNING block_time;""", {"blocktime": start_block_time})

            s.commit()

        try:
            result = result.all()
            return len(result), result[-1][0]
        except IndexError:
            return 0, None

    def update_outputs(self, start_block_time: int):
        with session_maker() as s:
            result = s.execute(f"""
            
                INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)
                
                (SELECT
                transactions.transaction_id as tid,
                transactions_outputs.script_public_key_address,
                transactions.block_time
                
                FROM transactions
                LEFT JOIN transactions_outputs ON transactions.transaction_id = transactions_outputs.transaction_id
                WHERE transactions.block_time >= :blocktime
                 ORDER by transactions.block_time ASC
                 LIMIT {LIMIT})
                
                 ON CONFLICT DO NOTHING
                 RETURNING block_time;""", {"blocktime": start_block_time})

            s.commit()

        try:
            result = result.all()
            return len(result), result[-1][0]
        except IndexError:
            return 0, None
