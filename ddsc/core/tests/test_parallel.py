from unittest import TestCase
from ddsc.core.parallel import WaitingTaskList, Task, TaskRunner, TaskExecutor


def no_op():
    pass


class NoOpTask(object):
    def __init__(self):
        self.func = no_op


class TestWaitingTaskList(TestCase):
    def task_ids(self, tasks):
        return [task.id for task in tasks]

    def test_get_next_tasks_two_tiers(self):
        task_list = WaitingTaskList()
        task_list.add(Task(1, None, NoOpTask()))
        task_list.add(Task(2, 1, NoOpTask()))
        task_list.add(Task(3, 1, NoOpTask()))

        none_task_ids = self.task_ids(task_list.get_next_tasks(None))
        self.assertEqual([1], none_task_ids)

        one_task_ids = self.task_ids(task_list.get_next_tasks(1))
        self.assertEqual([2, 3], one_task_ids)

        two_task_ids = self.task_ids(task_list.get_next_tasks(2))
        self.assertEqual([], two_task_ids)

    def test_get_next_tasks_one_tiers(self):
        task_list = WaitingTaskList()
        task_list.add(Task(1, None, NoOpTask()))
        task_list.add(Task(2, None, NoOpTask()))
        task_list.add(Task(3, None, NoOpTask()))

        none_task_ids = self.task_ids(task_list.get_next_tasks(None))
        self.assertEqual([1, 2, 3], none_task_ids)


class AddCommand(object):
    """
    Simple task that adds two numbers together returning the result.
    Run in a separate process to illustrate/test the parallel.TaskRunner.
    """
    def __init__(self, value1, value2):
        self.values = value1, value2
        self.parent_task_result = None
        self.result = None
        self.func = add_func

    def before_run(self, parent_task_result):
        self.parent_task_result = parent_task_result

    def create_context(self):
        return self.values

    def run(self, context):
        return context[0] + context[1]

    def after_run(self, results):
        self.result = results


def add_func(context):
    """
    Function run by AddCommand
    :param context: tuple of values passed in
    :return: sum of values
    """
    v1, v2 = context
    return v1 + v2


class TestTaskRunner(TestCase):
    """
    Task runner should be able to add numbers in a separate process and re-use the result in waiting tasks.
    """
    def test_single_add(self):
        add_command = AddCommand(10, 30)
        executor = TaskExecutor(10)
        runner = TaskRunner(executor)
        runner.add(None, add_command)
        runner.run()
        self.assertEqual(add_command.parent_task_result, None)
        self.assertEqual(add_command.result, 40)

    def test_two_adds_in_order(self):
        add_command = AddCommand(10, 30)
        add_command2 = AddCommand(4, 1)
        executor = TaskExecutor(10)
        runner = TaskRunner(executor)
        runner.add(None, add_command)
        runner.add(1, add_command2)
        runner.run()
        self.assertEqual(add_command.parent_task_result, None)
        self.assertEqual(add_command.result, 40)
        self.assertEqual(add_command2.parent_task_result, 40)
        self.assertEqual(add_command2.result, 5)

    def test_two_adds_in_parallel(self):
        add_command = AddCommand(10, 30)
        add_command2 = AddCommand(4, 1)
        executor = TaskExecutor(10)
        runner = TaskRunner(executor)
        runner.add(None, add_command,)
        runner.add(None, add_command2)
        runner.run()
        self.assertEqual(add_command.parent_task_result, None)
        self.assertEqual(add_command.result, 40)
        self.assertEqual(add_command2.parent_task_result, None)
        self.assertEqual(add_command2.result, 5)
