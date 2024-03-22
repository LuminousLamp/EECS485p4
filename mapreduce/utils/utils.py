
import socket
import json


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

        self.task_list: dict[int, dict] = {}

    def register_task(self, task_id: int):
        task_info = {
            "status": "unfinished",
        }
        assert task_id not in self.task_list
        self.task_list[task_id] = task_info

    def is_all_tasks_completed(self):
        for task_id, task_info in self.task_list.items():
            if task_info["status"] != "finished":
                return False
        return True
    
    def clear_task_list(self):
        self.task_list.clear()

    def mark_task_finished(self, task_id: int):
        assert task_id in self.task_list
        self.task_list[task_id]["status"] = "finished"
        

class W_info:
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