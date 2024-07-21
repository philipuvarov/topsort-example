
import asyncio
from topsort_example.scheduler import schedule
from topsort_example.task import Task


if __name__ == "__main__":
    task_c = Task(name="C",)
    task_b = Task(name="B", dependencies=[task_c])
    task_a = Task(name="A", dependencies=[task_b])

    task_d = Task(name="D")
    task_e = Task(name="E")
    
    tasks = [task_a, task_b, task_c, task_d, task_e]
    
    asyncio.run(schedule(tasks))