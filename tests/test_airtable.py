import asyncio
import pytest
import sys

from typing import List
from airbase.airtable import Airtable, Base

if sys.version_info[:2] < (3, 6):
    pass


@pytest.mark.asyncio
async def main() -> None:
    async with Airtable() as at:
        # Get all bases for a user
        await at.get_bases()
        assert isinstance(at.bases, List[Base])


if __name__ == "__main__":
    asyncio.run(main())
