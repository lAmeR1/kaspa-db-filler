from sqlalchemy import Column, String, Integer, BigInteger, Boolean, ARRAY

from dbsession import Base


class TxAddrMapping(Base):
    __tablename__ = 'tx_id_address_mapping'
    transaction_id = Column(String)  # "bedea078f74f241e7d755a98c9e39fda1dc56491dc7718485a8f221f73f03061",
    address = Column(String)
    block_time = Column(BigInteger)  # "1663286480803"
    is_accepted = Column(Boolean, default=False)
    id = Column(BigInteger, primary_key=True)