from unittest import TestCase
import queue
from ddsc.core.parallel import WaitingTaskList, Task, TaskRunner, TaskExecutor
from mock import patch, Mock


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


class AddCommandContext(object):
    def __init__(self, values, message_data, message_queue, task_id):
        self.values = values
        self.message_data = message_data
        self.message_queue = message_queue
        self.task_id = task_id

    def send_message(self, data):
        self.message_queue.put((self.task_id, data))


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
        self.send_message = None
        self.on_message_data = []

    def before_run(self, parent_task_result):
        self.parent_task_result = parent_task_result

    def create_context(self, message_queue, task_id):
        return AddCommandContext(self.values, self.send_message, message_queue, task_id)

    def after_run(self, results):
        self.result = results

    def on_message(self, data):
        self.on_message_data.append(data)


def add_func(context):
    """
    Function run by AddCommand
    :param context
    :return: sum of values
    """
    values = context.values
    if context.message_data:
        context.send_message(context.message_data)
    v1, v2 = values
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
        self.assertEqual(add_command.send_message, None)

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

    def test_command_with_message(self):
        add_command = AddCommand(10, 30)
        add_command.send_message = 'ok'
        add_command2 = AddCommand(4, 1)
        add_command2.send_message = 'waiting'
        executor = TaskExecutor(10)
        runner = TaskRunner(executor)
        runner.add(None, add_command,)
        runner.add(None, add_command2)
        runner.run()
        self.assertEqual(add_command.parent_task_result, None)
        self.assertEqual(add_command.result, 40)
        self.assertEqual(add_command2.parent_task_result, None)
        self.assertEqual(add_command2.result, 5)
        self.assertEqual(add_command.on_message_data, ['ok'])
        self.assertEqual(add_command2.on_message_data, ['waiting'])


class TestTaskExecutor(TestCase):
    @patch('ddsc.core.parallel.multiprocessing')
    def test_wait_for_tasks_with_multiple_messages(self, mock_multiprocessing):
        # Setup so we will go the the wait_for_tasks loop once and
        # the process_all_messages_in_queue inside the loop receives all the messages
        message_queue = Mock()
        message_queue.get_nowait.side_effect = [
            (1, "TEST"),
            (1, "TEST2"),
            queue.Empty,
            queue.Empty
        ]
        mock_multiprocessing.Manager.return_value.Queue.return_value = message_queue
        mock_pool = Mock()
        mock_pending_result = Mock()
        mock_pending_result.get.return_value = (1, 40)
        mock_pool.apply_async.side_effect = [
            mock_pending_result
        ]
        mock_multiprocessing.Pool.return_value = mock_pool

        add_command = AddCommand(10, 30)
        # Force messages to come in before the task exits
        # so we test 'wait_for_tasks' and not '
        add_command.send_message = 'testing'

        executor = TaskExecutor(2)
        executor.add_task(Task(1, None, add_command), None)
        executor.wait_for_tasks()
        self.assertEqual(40, add_command.result)
        self.assertEqual(add_command.on_message_data, ['TEST', 'TEST2'])

    @patch('ddsc.core.parallel.multiprocessing')
    def test_get_finished_results_with_multiple_messages(self, mock_multiprocessing):
        # Setup so we will go the the wait_for_tasks loop once and
        # get_finished_results will receive all the messages
        message_queue = Mock()
        message_queue.get_nowait.side_effect = [
            queue.Empty,
            (1, "TEST"),
            (1, "TEST2"),
            queue.Empty
        ]
        mock_multiprocessing.Manager.return_value.Queue.return_value = message_queue
        mock_pool = Mock()
        mock_pending_result = Mock()
        mock_pending_result.get.return_value = (1, 40)
        mock_pool.apply_async.side_effect = [
            mock_pending_result
        ]
        mock_multiprocessing.Pool.return_value = mock_pool

        add_command = AddCommand(10, 30)
        # Force messages to come in before the task exits
        # so we test 'wait_for_tasks' and not '
        add_command.send_message = 'testing'

        executor = TaskExecutor(2)
        executor.add_task(Task(1, None, add_command), None)
        executor.wait_for_tasks()
        self.assertEqual(40, add_command.result)
        self.assertEqual(add_command.on_message_data, ['TEST', 'TEST2'])
