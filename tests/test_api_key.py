import os
import pytest
import sys


from airbase.airtable import Airtable, Base  # noqa F401

if sys.version_info[:2] < (3, 6):
    pass


@pytest.mark.asyncio
async def test_api_key() -> None:
    async with Airtable() as at:
        # Get all bases for a user
        assert at.api_key == os.environ["AIRTABLE_API_KEY"]
