# -*- coding: utf-8 -*-

"""Delayed Bounded Semaphore for HTTP Connections"""

from asyncio import BoundedSemaphore, sleep
from collections import deque
from datetime import datetime

from inspect import iscoroutinefunction
from time import sleep as tsleep


class HTTPSemaphore(BoundedSemaphore):
    """ """

    def __init__(
        self,
        value: int = 10,
        interval: int = 60,  # in seconds
        max_calls: int = 300,
        **kwargs,
    ) -> None:
        """
        Bound calls / Rate Limit
        """  # noqa: E501
        self.rate = float(interval) / float(max_calls)
        # self.max = int(max_calls / interval) + 1
        self.interval = interval
        self.max = max_calls
        self.acquisitions = deque(maxlen=self.max)
        super().__init__(value, **kwargs)

    def throttle(self):
        if len(self.acquisitions) == self.max:
            first = self.acquisitions.popleft()
            last = self.acquisitions[-1]

            self.delta = (last - first).total_seconds()
            if self.delta <= self.interval:
                return True
            else:
                return False
        else:
            return False

    def time(self):
        remainder = self.interval - self.delta + 0.01
        # print(f"I have been delayed: {remainder} secs")
        return remainder

    def delay(func):
        async def inner_coro(self, *args, **kwargs):
            result = await func(self, *args, **kwargs)
            if self.throttle():
                await sleep(self.time())
            self.acquisitions.append(datetime.now())
            return result

        def inner_func(self, *args, **kwargs):
            result = func(self, *args, **kwargs)
            if self.throttle():
                tsleep(self.time())
            self.acquisitions.append(datetime.now())
            return result

        inner = inner_coro if iscoroutinefunction(func) else inner_func

        return inner

    acquire = delay(BoundedSemaphore.acquire)
