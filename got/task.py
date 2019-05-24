import abc


class BasicHTTPTask:

    def __init__(self, data):
        self.data = data

    async def before(self):
        pass

    async def on_task(self):
        pass

    async def handle(self):
        return True

    async def success(self):

        pass

    async def failure(self):
        pass
