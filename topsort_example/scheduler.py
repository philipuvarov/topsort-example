import asyncio
from pydantic import BaseModel
from graphlib import TopologicalSorter

from topsort_example.task import Task


class Result(BaseModel):
    task: Task
    success: bool
    exception: Exception | None = None

    class Config:
        # this for pydantic to allow arbitrary types, Exception in this case
        arbitrary_types_allowed = True


async def schedule(tasks: list[Task]):
    results = await _schedule(_prepare_graph(tasks))

    failed_results = [result for result in results if not result.success]
    succeeded_results = [result for result in results if result.success]
    print(
        f"Processed {len(succeeded_results)} tasks successfully and {len(failed_results)} tasks failed. Out of {len(tasks)} tasks."
    )


async def _schedule(graph: dict[Task, set[Task]]) -> list[Result]:
    sorter = TopologicalSorter(graph)
    sorter.prepare()

    results = []
    while sorter.is_active():
        tasks = []

        for task in sorter.get_ready():
            tasks.append(_execute(task))
        processed = await asyncio.gather(*tasks)

        for p in processed:
            sorter.done(p.task)
        results.extend(processed)

    return results


def _prepare_graph(tasks: list[Task]) -> dict[Task, set[Task]]:
    graph = {}
    for task in tasks:
        _flatten(task, graph)

    return graph


def _flatten(task: Task, graph: dict[Task, set[Task]]):
    if task not in graph:
        graph[task] = set()
    for dep in task.dependencies:
        graph[task].add(dep)
        _flatten(dep, graph)
        

async def _execute(task: Task) -> Result:
    try:
        await task.doWork()
        return Result(task=task, success=True)
    except Exception as e:
        return Result(task=task, success=False, exception=e)