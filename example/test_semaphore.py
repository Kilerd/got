import asyncio

semaphore = asyncio.Semaphore(3)


async def acquire():
    print("starting....")
    async with semaphore:
        print("acquire semaphore....")
        await asyncio.sleep(3)
        print("end....")


async def run():
    for i in range(10):
        asyncio.gather(acquire())


loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.gather(run()))
loop.run_forever()
loop.close()
