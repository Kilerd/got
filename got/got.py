import asyncio
from dataclasses import dataclass
from typing import Dict, Any

from got.broker import BasicBroker
from got.task import BasicHTTPTask


@dataclass
class TaskRuntimeManager:
    broker: BasicBroker
    task_class: BasicHTTPTask
    worker_limit: asyncio.Semaphore


class Got:

    def __init__(self, broker):
        """
        初始化
        :param broker: 使用哪种数据库作为任务储存介质
        """

        self.broker = broker

        self.tasks: Dict[str, TaskRuntimeManager] = dict()  # 用于储存爬虫任务类型

    def handle(self, task_name: str, worker_limit: int = -1):
        """
        添加爬虫任务类型
        :param task_name: 爬虫名字
        :param worker_limit: 这种任务最大同时执行任务个数
        :return: 装饰器
        """

        def decorator(task_class):
            self.tasks[task_name] = TaskRuntimeManager(
                self.broker, task_class, asyncio.Semaphore(worker_limit)
            )

        return decorator

    async def new(self, task: str, data: Any):
        await self.tasks.get(task).broker.push(data)

    async def task_serve(self, task: str, task_class: BasicHTTPTask, task_data: Any):
        """
        爬虫任务处理流程
        :param task: 任务类型
        :param task_class: 任务处理类
        :param task_data: 任务内容
        :return:
        """
        task_instance = task_class(task_data)  # 实例化任务处理类，同时把任务内容穿进去
        await task_instance.before()  # 执行 预处理函数
        ret = await task_instance.on_task()  # 执行 HTTP 请求

        # 如果 HTTP 请求成功
        if await task_instance.handle():  # 执行 handle 函数，实际上时处理用户需要抓取哪些数据
            await task_instance.success()
        else:
            await task_instance.failure()


    async def task_list_serve(self, task: str, restore=False):
        """
        监听一种任务类型
        :param task: 任务类型
        :param restore: 是否把未完成的任务重新回滚进「准备工作队列」
        :return:
        """

        print('listening on list {}'.format(task))
        task_info = self.tasks.get(task)  # 读取该种任务类型的信息

        while True:
            with (await task_info.worker_limit):
                task_data = await task_info.broker.get_task()  # 从数据库读取一个任务
                await self.task_serve(task, task_info.task_class, task_data)

    async def serve(self, tasks, restore=False):
        """
        异步启动系统
        :param tasks: 启动的任务类型
        :param restore: 是否把未完成的任务重新回滚进「准备工作队列」
        :return:
        """
        if not isinstance(tasks, list):
            tasks = [tasks]

        for task in tasks:
            if task in self.tasks:
                asyncio.gather(self.task_list_serve(task, restore))

    def serve_sync(self, tasks=None, restore=False):
        """
        同步启动爬虫系统
        :param tasks:
        :param restore:
        :return:
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(self.serve(tasks, restore)))
        loop.run_forever()
        loop.close()
