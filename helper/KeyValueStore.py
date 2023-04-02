# encoding: utf-8
from sqlalchemy import select, update, insert

from dbsession import session_maker
from models.Variable import KeyValueModel


def get(key):
    with session_maker() as s:
        result = s.execute(select(KeyValueModel.value).where(KeyValueModel.key == key))
        return result.scalar()


def set(key, value):
    with session_maker() as s:
        result = s.execute(update(KeyValueModel)
                                 .where(KeyValueModel.key == key)
                                 .values(value=value))

        if result.rowcount == 1:
            s.commit()
            return True

        result = s.execute(insert(KeyValueModel).values(key=key, value=value))

        s.commit()

        return True
