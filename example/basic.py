import asyncio

from got.broker import InMemoryBroker
from got.got import Got
from got.task import BasicHTTPTask
import random

got = Got(InMemoryBroker())


@got.handle("basic", 10)
class HTTPTask(BasicHTTPTask):

    def __init__(self, data):
        super().__init__(data)

    async def before(self):
        pass

    async def on_task(self):
        return await super().on_task()

    async def handle(self):
        await asyncio.sleep(random.randint(0, 5))
        return await super().handle()

    async def success(self):
        print(f"doing {self.data}")
        for i in range(random.randint(1, 3)):
            await got.new("basic", f"{self.data}.")

    async def failure(self):
        return await super().failure()


got.serve_sync(["basic"])
