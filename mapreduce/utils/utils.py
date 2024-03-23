"""utils."""

import socket
import json
from collections import deque

STATUS_FREE = 0
STATUS_READY = 0
STATUS_BUSY = 1
STATUS_DEAD = 2
STATUS_SHUTDOWN = 3


class Job:
    """Represents a job in the MapReduce system."""

    def __init__(
        self,
        id: int,
        input_directory: str,
        output_directory: str,
        mapper_executable: str,
        reducer_executable: str,
        num_mappers: int,
        num_reducers: int,
    ) -> None:
        """
        Initialize a Job object.

        Args:
            id (int): The ID of the job.
            input_directory (str): The input directory for the job.
            output_directory (str): The output directory for the job.
            mapper_executable (str): The path to the mapper executable.
            reducer_executable (str): The path to the reducer executable.
            num_mappers (int): The number of mappers for the job.
            num_reducers (int): The number of reducers for the job.
        """
        self.id: int = id
        self.input_directory: str = input_directory
        self.output_directory: str = output_directory
        self.mapper_executable: str = mapper_executable
        self.reducer_executable: str = reducer_executable
        self.num_mappers: int = num_mappers
        self.num_reducers: int = num_reducers

        self.task_list: dict[int, Task] = {}  # key: task_id, value: task_info
        self.task_queue: deque[int] = deque()  # element is task_id

    def register_task(self, task_id: int, task_info: dict) -> None:
        """
        Register a task for the job.

        Args:
            task_id (int): The ID of the task.
            task_info (dict): The information of the task.
        """
        assert task_id not in self.task_list
        self.task_list[task_id] = Task(task_id, task_info)
        self.task_queue.append(task_id)

    def next_task(self) -> tuple:
        """
        Retrieve the next task from the task queue.

        Returns:
            tuple: The task information.
        """
        assert len(self.task_queue) > 0
        next_task = self.task_list[self.task_queue.popleft()]
        return next_task.task_info

    def reset_task(self, task_id: int) -> None:
        """
        Reset a task to unfinished state and adds it back to the task queue.

        Args:
            task_id (int): The ID of the task.
        """
        assert task_id in self.task_list
        assert self.task_list[task_id].task_status == "unfinished"
        assert task_id not in self.task_queue

        self.task_queue.append(task_id)

    def have_pending_job(self) -> bool:
        """
        Check if there are pending tasks in the task queue.

        Returns:
            bool: True if there are pending tasks, False otherwise.
        """
        return len(self.task_queue) > 0

    def is_all_tasks_completed(self) -> bool:
        """
        Check if all tasks in the job are completed.

        Returns:
            bool: True if all tasks are completed, False otherwise.
        """
        for task in self.task_list.values():
            if task.task_status != "finished":
                return False
        self.task_list.clear()
        return True

    def mark_task_finished(self, task_id: int):
        """
        Mark a task as finished.

        Args:
            task_id (int): The ID of the task.
        """
        assert task_id in self.task_list
        assert task_id not in self.task_queue

        self.task_list[task_id].task_status = "finished"


class Task:
    """Represent a task in the MapReduce system."""

    def __init__(self, task_id: int, task_info: dict) -> None:
        """
        Initialize a Task object.

        Args:
            task_id (int): The ID of the task.
            task_info (dict): The information of the task.
        """
        self.task_id: int = task_id
        self.task_info: dict = task_info
        self.task_status = "unfinished"


class RemoteWorker:
    """Represent a remote worker in the MapReduce system."""

    def __init__(self, host: str, port: int) -> None:
        """
        Initialize a RemoteWorker object.

        Args:
            host (str): The host of the remote worker.
            port (int): The port of the remote worker.
        """
        self.host: str = host
        self.port: int = port

        self.missing_heartbeats: int = 0

        self.status: int = STATUS_READY
        self.curr_task: dict = None

    def assign_task(self, task_message: dict):
        """
        Assign a task to the remote worker.

        Args:
            task_message (dict): The task message to be assigned.
        """
        assert (
            self.status == STATUS_READY
        ), "the worker should be STATUS_READY, but it is not"
        assert self.curr_task is None
        self.status = STATUS_BUSY
        self.curr_task = task_message

    def clear_task(self):
        """Clear the current task assigned to the remote worker."""
        assert self.status != STATUS_READY
        assert self.curr_task is not None
        self.status = STATUS_READY
        self.curr_task = None

    def receive_hearbeat(self) -> None:
        """Receive a heartbeat from the remote worker."""
        self.missing_heartbeats = 0

    def record_heartbeat_miss(self) -> bool:
        """
        Record a heartbeat miss for the remote worker.

        Returns:
            bool: True if the number of heartbeat misses exceeds
            the threshold, False otherwise.
        """
        self.missing_heartbeats += 1

        return self.missing_heartbeats > 5


def tcp_receive_json(socket: socket.socket) -> dict:
    """
    Receive a JSON message over TCP.

    Args:
        socket (socket.socket): The TCP socket.

    Returns:
        dict: The received JSON message.
    """
    socket.settimeout(1)
    with socket:
        message_chunks = []
        while True:
            try:
                data = socket.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            message_chunks.append(data)

    message_bytes = b"".join(message_chunks)
    message_str = message_bytes.decode("utf-8")

    try:
        message_dict = json.loads(message_str)
    except json.JSONDecodeError:
        raise json.JSONDecodeError

    return message_dict


def udp_receive_json(sock: socket.socket) -> dict:
    """
    Receive a JSON message over UDP.

    Args:
        sock (socket.socket): The UDP socket.

    Returns:
        dict: The received JSON message.
    """
    try:
        message_bytes = sock.recv(4096)
    except socket.timeout:
        raise socket.timeout

    message_str = message_bytes.decode("utf-8")
    try:
        message_dict = json.loads(message_str)
    except json.JSONDecodeError:
        raise json.JSONDecodeError
    return message_dict
