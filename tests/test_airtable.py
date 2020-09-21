import pytest
import sys

from airbase.airtable import Airtable, Base

if sys.version_info[:2] < (3, 6):
    pass


@pytest.mark.asyncio
async def test_airtable() -> None:
    async with Airtable() as at:
        # Get all bases for a user
        await at.get_bases()
        assert getattr(at, "bases", None)
        assert isinstance(at.bases[0], Base)
