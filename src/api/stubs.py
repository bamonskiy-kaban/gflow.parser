from core.broker import broker


@broker.task(task_name="worker.tasks:process_function")
async def process_function_stub():
    pass


@broker.task(task_name="worker.tasks:acquire_target_info")
async def acquire_target_info_stub():
    pass
