import asyncio
from pydantic import BaseModel

class Task(BaseModel):
    name: str
    dependencies: list['Task'] = []

    async def doWork(self):
        print(f"Working on {self.name}")
        await asyncio.sleep(1)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name
