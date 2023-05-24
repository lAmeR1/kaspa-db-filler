# encoding: utf-8

import logging
import time
from datetime import datetime, timedelta

from dbsession import session_maker
from helper import KeyValueStore
from models.TxAddrMapping import TxAddrMapping

LIMIT = 1000
PRECONDITION_RETRIES = 2

_logger = logging.getLogger(__name__)


class TxAddrMappingUpdater(object):
    def __init__(self):
        self.last_block_time = None
        self.id_counter_inputs = None
        self.id_counter_outputs = None

    def precondition(self):
        with session_maker() as s:
            self.id_counter_inputs = int(KeyValueStore.get("last_id_counter_inputs") or 0)
            self.id_counter_outputs = int(KeyValueStore.get("last_id_counter_outputs") or 0)

    @staticmethod
    def minimum_timestamp():
        return round((datetime.now() - timedelta(minutes=1)).timestamp() * 1000)

    def loop(self):
        self.precondition()

        error_cnt = 0

        _logger.debug('Start TxAddrMappingUpdater')  # type: TxAddrMapping

        while True:

            # get max id ( either LIMIT or maximum in DB )
            with session_maker() as s:
                max_in = min(self.id_counter_inputs + LIMIT,
                             s.execute(
                                 f"""SELECT id FROM transactions_inputs ORDER by id DESC LIMIT 1""")
                             .scalar() or 0)

                max_out = min(self.id_counter_outputs + LIMIT,
                              s.execute(
                                  f"""SELECT id FROM transactions_outputs ORDER by id DESC LIMIT 1""")
                              .scalar() or 0)

            try:
                count_outputs, new_last_block_time_outputs = self.update_outputs(self.id_counter_outputs,
                                                                                 max_out)
                count_inputs, new_last_block_time_inputs = self.update_inputs(self.id_counter_inputs,
                                                                              max_in)
                # save last runs ids in case of restart
                KeyValueStore.set("last_id_counter_inputs", max_in)
                KeyValueStore.set("last_id_counter_outputs", max_out)

            except Exception:
                error_cnt += 1
                if error_cnt <= 3:
                    time.sleep(10)
                    continue
                raise

            _logger.info(f"Updated {count_inputs} input mappings.")
            _logger.info(f"Updated {count_outputs} outputs mappings.")

            last_id_counter_inputs = self.id_counter_inputs
            last_id_counter_outputs = self.id_counter_outputs

            # next start id is the maximum of last request
            self.id_counter_inputs = max_in
            self.id_counter_outputs = max_out

            _logger.debug(f"Next TX-Input ID: {self.id_counter_inputs}." +
                          (f" ({datetime.fromtimestamp(new_last_block_time_inputs / 1000).isoformat()})"
                           if new_last_block_time_inputs else ""))

            _logger.debug(f"Next TX-Output ID: {self.id_counter_outputs}." +
                          (f" ({datetime.fromtimestamp(new_last_block_time_outputs / 1000).isoformat()})"
                           if new_last_block_time_outputs else ""))

            if last_id_counter_inputs + LIMIT > self.id_counter_inputs and \
                    last_id_counter_outputs + LIMIT > self.id_counter_outputs:
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

    def update_inputs(self, min_id: int, max_id: int):
        with session_maker() as s:

            result = s.execute(f"""INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)

                SELECT DISTINCT * FROM (
                    SELECT transactions_inputs.transaction_id,
                           transactions_outputs.script_public_key_address,
                           transactions.block_time FROM transactions_inputs 
                    LEFT JOIN transactions_outputs ON 
                    
                        transactions_outputs.transaction_id = transactions_inputs.previous_outpoint_hash AND
                        transactions_outputs.index = transactions_inputs.previous_outpoint_index
                    
                    LEFT JOIN transactions ON transactions.transaction_id = transactions_inputs.transaction_id
                        
                    WHERE transactions_inputs.id > :minId AND transactions_inputs.id <= :maxId
                       AND transactions_outputs.script_public_key_address IS NOT NULL
                    ORDER by transactions_inputs.id
                    ) as distinct_query
                    
                 ON CONFLICT DO NOTHING
                         RETURNING block_time;""", {"minId": min_id, "maxId": max_id})

            s.commit()

        try:
            result = result.all()
            return len(result), result[-1][0]
        except IndexError:
            return 0, None

    def update_outputs(self, min_id: int, max_id: int):
        with session_maker() as s:
            result = s.execute(f"""
            
                INSERT INTO tx_id_address_mapping (transaction_id, address, block_time)
                
                (SELECT sq.*, transactions.block_time FROM (SELECT transaction_id, script_public_key_address                 
                FROM transactions_outputs
                WHERE transactions_outputs.id > :minId and transactions_outputs.id <= :maxId
                ORDER by transactions_outputs.id DESC) as sq
				JOIN transactions ON transactions.transaction_id = sq.transaction_id)
                
                 ON CONFLICT DO NOTHING
                 RETURNING block_time;""", {"minId": min_id, "maxId": max_id})

            s.commit()

        try:
            result = result.all()
            return len(result), result[-1][0]
        except IndexError:
            return 0, None
