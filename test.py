import asyncio
from airbase import Airtable


async def main() -> None:
    async with Airtable() as at:
        # Get all bases for a user
        await at.get_bases()
        for base in at.bases:
            print(base.name)


if __name__ == "__main__":
    asyncio.run(main())
