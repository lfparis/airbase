from asyncio import create_task, gather
from functools import wraps
from typing import Callable

MAX_CHUNK_SIZE = 10


def chunkify(func: Callable):
    @wraps(func)
    async def inner(self, *args, **kwargs):
        records = kwargs["records"]
        method = kwargs["method"]
        typecast = kwargs.get("typecast") or False

        records_iter = (
            records[i : i + MAX_CHUNK_SIZE]
            for i in range(0, len(records), MAX_CHUNK_SIZE)
        )

        tasks = []
        for sub_list in records_iter:
            tasks.append(
                create_task(func(self, method, sub_list, typecast))
            )
        task_return_values = await gather(*tasks)

        unpacked_results = []
        for task_return_value in task_return_values:
            if task_return_value.get("records"):
                unpacked_results.extend(task_return_value["records"])
            else:
                unpacked_results.append(task_return_value)
        return unpacked_results

    return inner
