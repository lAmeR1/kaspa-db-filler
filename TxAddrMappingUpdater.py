# encoding: utf-8

import logging
import os
import time
from datetime import datetime, timedelta

from dbsession import session_maker

_logger = logging.getLogger(__name__)


class TxAddrMappingUpdater(object):
    def __init__(self):
        self.last_block_time = 1680096059000

    @staticmethod
    def minimum_timestamp():
        return round((datetime.now() - timedelta(minutes=1)).timestamp() * 1000)

    def loop(self):
        error_cnt = 0
        i = 0
        while True:
            i += 1

            if i == 5:
                raise Exception('und jetzt?')
            try:
                count_outputs, new_last_block_time_outputs = t.update_outputs(self.last_block_time)
                count_inputs, new_last_block_time_inputs = t.update_inputs(self.last_block_time)
            except Exception:
                error_cnt += 1
                if error_cnt <= 3:
                    time.sleep(10)
                    continue
                raise Exception('Exception occured updating TxAddrMapping table')

            print(f"Updated {count_inputs} input mappings.")
            print(f"Updated {count_outputs} outputs mappings.")

            # initialize
            new_last_block_time = new_last_block_time_inputs

            # fallback if nothing added
            if not new_last_block_time_outputs and not new_last_block_time_inputs:
                print("No mapping to be added.")
                new_last_block_time = t.get_last_block_time(self.last_block_time)

            # get minimum timestamp to check next loop
            # -> block_time is not an consistent increasing value -> need to leave space in the past
            # -> now() - 1 min
            min_timestamp = TxAddrMappingUpdater.minimum_timestamp()

            # new last block times are sorted in the database!
            self.last_block_time = min(new_last_block_time_outputs or new_last_block_time,
                                       min_timestamp,
                                       new_last_block_time_inputs or new_last_block_time)

            _logger.debug(datetime.fromtimestamp(self.last_block_time / 1000).isoformat())
            print(datetime.fromtimestamp(self.last_block_time / 1000).isoformat())

            if self.last_block_time == min_timestamp:
                # updater catched up. You have time
                time.sleep(10)

    def get_last_block_time(self, start_block_time):
        with session_maker() as s:
            result = s.execute(f"""SELECT
                transactions.block_time
                
                FROM transactions
                WHERE transactions.block_time > :blocktime
                 ORDER by transactions.block_time ASC
                 LIMIT 400""", {"blocktime": start_block_time}).all()

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
         WHERE transactions.block_time > {start_block_time}) as sq

        LEFT JOIN transactions_inputs ON transactions_inputs.transaction_id = sq.tid
        LEFT JOIN transactions_outputs ON transactions_outputs.transaction_id = transactions_inputs.previous_outpoint_hash AND  transactions_outputs.index = transactions_inputs.previous_outpoint_index::int
         ORDER by sq.block_time ASC
         LIMIT 400) as masterq
         WHERE script_public_key_address IS NOT NULL)

         ON CONFLICT DO NOTHING
         RETURNING block_time;""")

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
                WHERE transactions.block_time > {start_block_time}
                 ORDER by transactions.block_time ASC
                 LIMIT 400)
                
                 ON CONFLICT DO NOTHING
                 RETURNING block_time;""")

            s.commit()

        try:
            result = result.all()
            return len(result), result[-1][0]
        except IndexError:
            return 0, None


if __name__ == '__main__':
    # custom exception hook
    def custom_hook(args):
        # report the failure
        print(f'Thread failed: {args.exc_value}')
        os._exit(1)


    t = TxAddrMappingUpdater()

    import threading

    # set the exception hook
    threading.excepthook = custom_hook
    p = threading.Thread(target=t.loop, daemon=True)

    p.start()

    while True:
        print("bin da")
        time.sleep(1)
