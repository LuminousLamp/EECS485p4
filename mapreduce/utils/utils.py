
import socket
import json
from collections import deque


STATUS_FREE = 0
STATUS_READY = 0
STATUS_BUSY = 1
STATUS_DEAD = 2
STATUS_SHUTDOWN = 3

class Job:
    def __init__(self, id:int, input_directory: str, output_directory: str, mapper_executable: str, reducer_executable: str, num_mappers: int, num_reducers: int) -> None:
        self.id:int = id
        self.input_directory: str = input_directory
        self.output_directory: str = output_directory
        self.mapper_executable: str = mapper_executable
        self.reducer_executable: str = reducer_executable
        self.num_mappers: int = num_mappers
        self.num_reducers: int = num_reducers

        self.task_list: dict[int, Task] = {} # key: task_id, value: task_info
        self.task_queue: deque[int] = deque() # element is task_id

    def register_task(self, task_id: int, task_info: dict) -> None:

        assert task_id not in self.task_list
        self.task_list[task_id] = Task(task_id, task_info)
        self.task_queue.append(task_id)
    
    def next_task(self) -> tuple:
        assert len(self.task_queue) > 0
        next_task = self.task_list[self.task_queue.popleft()]
        return next_task.task_info

    def reset_task(self, task_id: int) -> None:
        assert task_id in self.task_list
        assert self.task_list[task_id].task_status == "unfinished"
        assert task_id not in self.task_queue

        self.task_queue.append(task_id)

    def have_pending_job(self) -> bool:
        return len(self.task_queue) > 0
    
    def is_all_tasks_completed(self) -> bool:
        # print(f"there are {len(self.task_list)} tasks")
        for task in self.task_list.values():
            if task.task_status != "finished":
                return False
        self.task_list.clear()
        return True

    def mark_task_finished(self, task_id: int):
        assert task_id in self.task_list
        assert task_id not in self.task_queue

        self.task_list[task_id].task_status = "finished"

class Task:
    def __init__(self, task_id:int, task_info: dict) -> None:
        self.task_id: int = task_id
        self.task_info: dict = task_info
        self.task_status = "unfinished"

class RemoteWorker:
    def __init__(self, host:str, port:int) -> None:
        self.host: str = host
        self.port: int = port
        
        self.missing_heartbeats: int = 0

        self.status: int = STATUS_READY
        self.curr_task: dict = None

    def assign_task(self, task_message: dict):
        assert self.status == STATUS_READY, "the worker should be STATUS_READY, but it is not"
        assert self.curr_task == None
        self.status = STATUS_BUSY
        self.curr_task = task_message

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            message = json.dumps(task_message)
            sock.sendall(message.encode('utf-8'))

    def clear_task(self):
        assert self.status == STATUS_BUSY
        assert self.curr_task != None
        self.status = STATUS_READY
        self.curr_task = None

    def receive_hearbeat(self) -> None:
        self.missing_heartbeats = 0

    def record_heartbeat_miss(self) -> bool:
        self.missing_heartbeats += 1

        return self.missing_heartbeats > 5


def tcp_receive_json(socket: socket.socket) -> dict:
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
    
    message_bytes = b''.join(message_chunks)
    message_str = message_bytes.decode("utf-8")

    try:
        message_dict = json.loads(message_str)
    except json.JSONDecodeError:
        raise json.JSONDecodeError

    return message_dict

def udp_receive_json(sock: socket.socket) -> dict:
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