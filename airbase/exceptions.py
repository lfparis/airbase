from aiohttp import ClientResponse
from typing import Optional


class AirbaseException(Exception):
    """AirbaseException"""

    pass


class AirbaseResponseException(AirbaseException):
    """AirbaseResponseException"""

    def __init__(self, *args, response: Optional[ClientResponse] = None):
        super().__init__(*args)
