"""
Allows user to build up a series of dependant parallel tasks.
TaskRunner executes a list of Tasks in parallel based on how many processes can be run at once.
Each Task consists of a unique_id, an task_id that it will wait for before running and a Command to execute.
Each Command contains a function pointer to a global function to be run in the background and some
setup/cleanup methods that will be run in the foreground.
"""

from multiprocessing import Pool
from collections import deque
import traceback
import sys


class Task(object):
    """
    Represents a task that has a unique task id, a command specifying foreground code to run and
    a function that will be run in a background process.
    Command must have similar interface with before_run, create_context and after_run.
    """
    def __init__(self, task_id, wait_for_task_id, command):
        """
        Setup task so it can be executed.
        :param task_id: int: unique id of this task
        :param wait_for_task_id: int: unique id of the task that this one is waiting for
        :param command: object with foreground setup/teardown methods and background function
        """
        self.id = task_id
        self.wait_for_task_id = wait_for_task_id
        self.command = command
        self.func = command.func

    def before_run(self, parent_task_result):
        """
        Run in main process before run method.
        :param parent_task_result: object: result of previous task or None if no previous task
        """
        self.command.before_run(parent_task_result)

    def create_context(self):
        """
        Run serially before the run method.
        :return object: context object passing state to the thread
        """
        return self.command.create_context()

    def after_run(self, results):
        """
        Run in main process after run method.
        :param results: object: results from run method.
        """
        return self.command.after_run(results)


class WaitingTaskList(object):
    """
    List of pending tasks with lookup based on what task id they are waiting on.
    """
    def __init__(self):
        self.wait_id_to_task = {}

    def add(self, task):
        """
        Add this task to the lookup based on it's wait_for_task_id property.
        :param task: Task: task to add to the list
        """
        wait_id = task.wait_for_task_id
        task_list = self.wait_id_to_task.get(wait_id, [])
        task_list.append(task)
        self.wait_id_to_task[wait_id] = task_list

    def get_next_tasks(self, finished_task_id):
        """
        Return list of tasks that were waiting for finished_task_id.
        :param finished_task_id: int: task id for some task that has just finished
        :return: [Task]: tasks waiting for finished_task_id
        """
        return self.wait_id_to_task.get(finished_task_id, [])


class TaskRunner(object):
    """
    Runs a bunch of tasks in parallel with support for task waiting.
    """
    def __init__(self, executor):
        """
        Setup runner to use executor to run it's tasks.
        :param executor: TaskExecutor: actually executes tasks and returns their results
        """
        self.waiting_task_list = WaitingTaskList()
        self.executor = executor
        self.next_id = 1

    def _claim_next_id(self):
        """
        Convinience method to generate sequential ids for tasks.
        :return: int: numeric ids representing each unique task
        """
        next_id = self.next_id
        self.next_id += 1
        return next_id

    def add(self, parent_task_id, command):
        """
        Create a task for the command that will wait for parent_task_id before starting.
        :param parent_task_id: int: id of task to wait for or None if it can start immediately
        :param command: TaskCommand: contains data function to run
        :return: int: task id we created for this command
        """
        task_id = self._claim_next_id()
        self.waiting_task_list.add(Task(task_id, parent_task_id, command))
        return task_id

    def get_next_tasks(self, finished_task_id):
        """
        Get the next set of tasks for a finished_task_id
        :param finished_task_id: int: task id of a task that finished
        :return: [Task]: tasks that were waiting for the finished_task_id task to finish
        """
        return self.waiting_task_list.get_next_tasks(None)

    def run(self):
        """
        Runs all tasks in this runner on the executor.
        Blocks until all tasks have been completed.
        :return:
        """
        for task in self.get_next_tasks(None):
            self.executor.add_task(task, None)
        while not self.executor.is_done():
            done_task_and_result = self.executor.wait_for_tasks()
            for task, task_result in done_task_and_result:
                self._add_sub_tasks_to_executor(task, task_result)

    def _add_sub_tasks_to_executor(self, parent_task, parent_task_result):
        """
        Add all subtasks for parent_task to the executor.
        :param parent_task: Task: task that has just finished
        :param parent_task_result: object: result of task that is finished
        """
        for sub_task in self.waiting_task_list.get_next_tasks(parent_task.id):
            self.executor.add_task(sub_task, parent_task_result)


class TaskExecutor(object):
    """
    Executes tasks in a pool of processes.
    """
    def __init__(self, tasks_at_once):
        """
        Setup to run tasks in background limiting to tasks_at_once processes.
        :param tasks_at_once: int: number of tasks we can run at once
        """
        self.pool = Pool()
        self.tasks = deque()
        self.task_id_to_task = {}
        self.pending_results = []
        self.tasks_at_once = tasks_at_once

    def add_task(self, task, parent_task_result):
        """
        Add a task to run with the specified result from this tasks parent(can be None)
        :param task: Task: task that should be run
        :param parent_task_result: object: value to be passed to task for setup
        """
        self.tasks.append((task, parent_task_result))
        self.task_id_to_task[task.id] = task

    def is_done(self):
        """
        Have we exhausted all tasks.
        :return: bool: True if we have finished all tasks and their pending results
        """
        return not self._has_more_tasks() and not self._has_more_pending_results()

    def _has_more_tasks(self):
        return len(self.tasks) > 0

    def _has_more_pending_results(self):
        return len(self.pending_results) > 0

    def wait_for_tasks(self):
        """
        Wait for one or more tasks to finish or return empty list if we are done.
        Starts new tasks if we have less than task_at_once currently running.
        :return: [(Task,object)]: list of (task,result) for finished tasks
        """
        finished_tasks_and_results = []
        while len(finished_tasks_and_results) == 0:
            if self.is_done():
                break
            self.start_tasks()
            finished_tasks_and_results = self.get_finished_results()
        return finished_tasks_and_results

    def start_tasks(self):
        """
        Start however many tasks we can based on our limits and what we have left to finish.
        """
        while self.tasks_at_once > len(self.pending_results) and self._has_more_tasks():
            task, parent_result = self.tasks.popleft()
            self.execute_task(task, parent_result)

    def execute_task(self, task, parent_result):
        """
        Run a single task in another process saving the result to our list of pending results.
        :param task: Task: function and data we can run in another process
        :param parent_result: object: result from our parent task
        """
        task.before_run(parent_result)
        context = task.create_context()
        pending_result = self.pool.apply_async(execute_task_async, (task.func, task.id, context))
        self.pending_results.append(pending_result)

    def get_finished_results(self):
        """
        Go through pending results and retrieve the results if they are done.
        Then start child tasks for the task that finished.
        """
        task_and_results = []
        for pending_result in self.pending_results:
            if pending_result.ready():
                ret = pending_result.get()
                task_id, result = ret
                task = self.task_id_to_task[task_id]
                task.after_run(result)
                task_and_results.append((task, result))
                self.pending_results.remove(pending_result)
        return task_and_results


def execute_task_async(task_func, task_id, context):
    """
    Global function run for Task. multiprocessing requires a top level function.
    :param task_func: function: function to run (must be pickle-able)
    :param task_id: int: unique id of this task
    :param context: object: single argument to task_func (must be pickle-able)
    :return: (task_id, object): return passed in task id and result object
    """
    try:
        result = task_func(context)
        return task_id, result
    except:
        # Put all exception text into an exception and raise that so main process will print this out
        raise Exception("".join(traceback.format_exception(*sys.exc_info())))


