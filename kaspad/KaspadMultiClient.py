# encoding: utf-8
import asyncio

from kaspad.KaspadClient import KaspadClient
# pipenv run python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/rpc.proto ./protos/messages.proto ./protos/p2p.proto
from kaspad.KaspadThread import KaspadCommunicationError


class KaspadMultiClient(object):
    def __init__(self, hosts: list[str]):
        self.kaspads = [KaspadClient(*h.split(":")) for h in hosts]

    def __get_kaspad(self):
        for k in self.kaspads:
            if k.is_utxo_indexed and k.is_synced:
                return k

    async def initialize_all(self):
        tasks = [asyncio.create_task(k.ping()) for k in self.kaspads]

        for t in tasks:
            await t

    async def request(self, command, params=None, timeout=60):
        try:
            return await self.__get_kaspad().request(command, params, timeout=timeout, retry=1)
        except KaspadCommunicationError:
            await self.initialize_all()
            return await self.__get_kaspad().request(command, params, timeout=timeout, retry=3)

    async def notify(self, command, params, callback):
        return await self.__get_kaspad().notify(command, params, callback)
