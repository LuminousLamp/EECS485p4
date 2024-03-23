import os
import tempfile
import logging
import json
import time
import click
import threading
import socket
from mapreduce.utils.utils import *
from collections import deque
import shutil
from pathlib import Path

"""MapReduce framework Manager node."""
# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""
    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        # Initialize Manager instance
        # attributes
        self.host: str = host
        self.port: int = port
        self.status: int = STATUS_FREE
        self.workers: dict[tuple, RemoteWorker] = {}
        self.workers_free: list[tuple] = []
        self.num_jobs: int = 0
        self.job_queue: deque = deque()
        self.curr_job: Job = None
        self.lock_workers = threading.Lock()
        self.cv_workers = threading.Condition(self.lock_workers)
        self.lock_jobs = threading.Lock()
        self.cv_jobs = threading.Condition(self.lock_jobs)
        # start threads
        threads = [
            threading.Thread(target=self.listen_message),
            threading.Thread(target=self.listen_heartbeat),
            threading.Thread(target=self.runjob),
            threading.Thread(target=self.faulttolerance),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def listen_message(self):
        """Main TCP socket listening for messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)
            while self.status != STATUS_SHUTDOWN:
                # try connection
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                except Exception:
                    continue
                # get message dict
                try:
                    message_dict = tcp_receive_json(clientsocket)
                    message_type = message_dict["message_type"]
                except Exception:
                    continue
                # execute according function
                if message_type == "shutdown":
                    self.shutdown()
                    break
                elif message_type == "register":
                    self.register_worker(
                        (message_dict["worker_host"],
                         int(message_dict["worker_port"]))
                    )
                elif message_type == "new_manager_job":
                    self.create_job(
                        message_dict["input_directory"],
                        message_dict["output_directory"],
                        message_dict["mapper_executable"],
                        message_dict["reducer_executable"],
                        int(message_dict["num_mappers"]),
                        int(message_dict["num_reducers"]),
                    )
                elif message_type == "finished":
                    self.mark_task_finished(
                        message_dict["task_id"],
                        message_dict["worker_host"],
                        message_dict["worker_port"],
                    )
                else:
                    continue

    def shutdown(self):
        """Shutdown the Manager and notify workers to shutdown."""
        self.status = STATUS_SHUTDOWN
        with self.lock_workers:
            self.cv_workers.notify_all()
            for worker_addr, worker in self.workers.items():
                if worker.status != STATUS_DEAD:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        try:
                            sock.connect(worker_addr)
                        except ConnectionRefusedError:
                            with self.lock_workers:
                                self._mark_worker_dead(worker_addr)
                            continue
                        shutdown_message = json.dumps({"message_type": "shutdown"})
                        sock.sendall(shutdown_message.encode("utf-8"))
        with self.lock_jobs:
            self.cv_jobs.notify_all()

    def register_worker(self, worker_addr: tuple):
        """Register a worker with the Manager."""
        with self.lock_workers:
            self.workers[worker_addr] = RemoteWorker(*worker_addr)
            assert worker_addr not in self.workers_free
            self.workers_free.append(worker_addr)
            self.cv_workers.notify_all()
        # send register ack mssg to the worker
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(worker_addr)
                registar_ack_message = json.dumps({"message_type": "register_ack"})
                sock.sendall(registar_ack_message.encode("utf-8"))
            except ConnectionRefusedError:
                with self.lock_workers:
                    self._mark_worker_dead(worker_addr)

    def create_job(
        self,
        input_directory,
        output_directory,
        mapper_executable,
        reducer_executable,
        num_mappers,
        num_reducers,
    ):
        """Create a new job and add it to the job queue."""
        with self.lock_jobs:
            new_job: Job = Job(
                self.num_jobs,
                input_directory,
                output_directory,
                mapper_executable,
                reducer_executable,
                num_mappers,
                num_reducers,
            )
            self.job_queue.append(new_job)
            self.cv_jobs.notify_all()
            self.num_jobs += 1

    def _assign_task_to_worker(self, worker_addr: tuple, task_info: dict):
        """Assign a task to a worker and send the task information to the worker."""
        # assumption: any functions that call this function should have self.lock_workers acquired
        assert self.lock_workers.locked()
        self.workers[worker_addr].assign_task(task_info)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(worker_addr)
                message = json.dumps(task_info)
                sock.sendall(message.encode("utf-8"))
            except ConnectionRefusedError:
                self._mark_worker_dead(worker_addr)

    def mark_task_finished(self, task_id: int, worker_host: str, worker_port: int):
        """Mark a task as finished and notify the Manager."""
        with self.lock_workers:
            # mark the task as finished
            self.curr_job.mark_task_finished(task_id)
            self.workers[(worker_host, worker_port)].clear_task()
            # worker back to ready
            self.workers_free.append((worker_host, worker_port))
            self.cv_workers.notify_all()

    def _mark_worker_dead(self, worker_addr):
        """Mark a worker as dead and handle its tasks."""
        # if it is free, just remove it from free list
        if self.workers[worker_addr].curr_task is None:
            assert worker_addr in self.workers_free
            self.workers_free = [
                worker for worker in self.workers_free if worker != worker_addr
            ]
        # otherwise, reset the task on the current job
        else:
            worker_task = self.workers[worker_addr].curr_task
            self.workers[worker_addr].clear_task()
            assert worker_addr not in self.workers_free
            self.curr_job.reset_task(worker_task["task_id"])
        self.workers[worker_addr].status = STATUS_DEAD

    def runjob(self):
        """Thread: Run a job."""
        while self.status != STATUS_SHUTDOWN:
            # The Manager runs each job to completion before starting a new job.
            with self.cv_jobs:
                while not (len(self.job_queue) > 0 or self.status == STATUS_SHUTDOWN):
                    self.cv_jobs.wait()
                if self.status == STATUS_SHUTDOWN:
                    return
                # start a new job
                assert (
                    self.status == STATUS_FREE
                ), "the manager status should be STATUS_FREE upon executing a job"
                assert len(self.job_queue) > 0
                self.status = STATUS_BUSY
                self.curr_job: Job = self.job_queue.popleft()
            # delete the output directory if it already exists, create the output directory
            if os.path.exists(self.curr_job.output_directory):
                shutil.rmtree(self.curr_job.output_directory)
            output_dir = self.curr_job.output_directory
            os.makedirs(output_dir)
            prefix = f"mapreduce-shared-job{self.curr_job.id:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                # -------- M A P --------#
                # 1. Divide the input files into num_mappers partitions using round robin
                input_files = sorted(os.listdir(self.curr_job.input_directory))
                partitions = [[] for _ in range(self.curr_job.num_mappers)]
                for task_id, input_files in enumerate(input_files):
                    partitions[task_id % self.curr_job.num_mappers].append(input_files)
                # 2. register all tasks (num_tasks == num_mappers)
                for task_id in range(self.curr_job.num_mappers):
                    task_info = {
                        "message_type": "new_map_task",
                        "task_id": task_id,
                        "input_paths": [
                            os.path.join(self.curr_job.input_directory, file)
                            for file in partitions[task_id]
                        ],
                        "executable": self.curr_job.mapper_executable,
                        "output_directory": tmpdir,
                        # The output_directory in the Map Stage will always be the Manager’s temporary directory
                        "num_partitions": self.curr_job.num_reducers,
                    }
                    self.curr_job.register_task(task_id, task_info)
                # 3. send all pending tasks
                while True:
                    while not (
                        self.curr_job.have_pending_job()
                        or self.curr_job.is_all_tasks_completed()
                        or self.status == STATUS_SHUTDOWN
                    ):
                        time.sleep(0.1)
                    if self.curr_job.is_all_tasks_completed():
                        break
                    if self.status == STATUS_SHUTDOWN:
                        return
                    task_info = self.curr_job.next_task()  # get the next pending task
                    with self.cv_workers:
                        while not (
                            len(self.workers_free) > 0 or self.status == STATUS_SHUTDOWN
                        ):  # wait if no workers available
                            self.cv_workers.wait()
                        if self.status == STATUS_SHUTDOWN:
                            return
                        worker_addr = self.workers_free.pop(0)  # get a free worker
                        self._assign_task_to_worker(
                            worker_addr, task_info
                        )  # and send the worker the task
                if self.status == STATUS_SHUTDOWN:
                    return
                # 4. wait until all map tasks run into completion, then start reducing
                while not (
                    self.curr_job.is_all_tasks_completed()
                    or self.status == STATUS_SHUTDOWN
                ):
                    time.sleep(0.1)
                if self.status == STATUS_SHUTDOWN:
                    return
                # -------- R E D U C E  -------- #
                # 1. register all tasks (num_tasks == num_mreducers)
                for task_id in range(self.curr_job.num_reducers):
                    tmpdir_path = Path(tmpdir)
                    intermediate_files = [
                        file.as_posix()
                        for file in tmpdir_path.glob(f"maptask*-part{task_id:05d}")
                    ]
                    intermediate_files.sort()
                    task_info = {
                        "message_type": "new_reduce_task",
                        "task_id": task_id,
                        "input_paths": intermediate_files,
                        "executable": self.curr_job.reducer_executable,
                        "output_directory": self.curr_job.output_directory,
                    }
                    self.curr_job.register_task(task_id, task_info)
                # 2. send all pending tasks
                while True:
                    while not (
                        self.curr_job.have_pending_job()
                        or self.curr_job.is_all_tasks_completed()
                        or self.status == STATUS_SHUTDOWN
                    ):
                        time.sleep(0.1)
                    if self.curr_job.is_all_tasks_completed():
                        break
                    if self.status == STATUS_SHUTDOWN:
                        return
                    task_info = self.curr_job.next_task()  # get the next pending task
                    with self.cv_workers:
                        while not (
                            len(self.workers_free) > 0 or self.status == STATUS_SHUTDOWN
                        ):  # wait if no workers available
                            self.cv_workers.wait()
                        if self.status == STATUS_SHUTDOWN:
                            return
                        worker_addr = self.workers_free.pop(0)  # get a free worker
                        self._assign_task_to_worker(
                            worker_addr, task_info
                        )  # and send the worker the task
                if self.status == STATUS_SHUTDOWN:
                    return
                # 3. wait until all reduce tasks run into completion
                while not (
                    self.curr_job.is_all_tasks_completed()
                    or self.status == STATUS_SHUTDOWN
                ):
                    time.sleep(0.1)
                if self.status == STATUS_SHUTDOWN:
                    return
            # now the current job is completed
            assert self.status != STATUS_FREE
            self.curr_job = None
            self.status = STATUS_FREE

    def listen_heartbeat(self):
        """UDP socket listening for heartbeats."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.settimeout(1)
            while self.status != STATUS_SHUTDOWN:
                try:
                    message_dict = udp_receive_json(sock)
                    message_type = message_dict["message_type"]
                    worker_host = message_dict["worker_host"]
                    worker_port = message_dict["worker_port"]
                except Exception:
                    continue
                if message_type != "heartbeat":
                    continue
                # ignore heartbeats from unregistered workers
                worker_addr = (worker_host, worker_port)
                with self.lock_workers:
                    if (
                        worker_addr not in self.workers
                        or self.workers[worker_addr].status == STATUS_DEAD
                    ):
                        continue
                    self.workers[worker_addr].receive_hearbeat()

    def faulttolerance(self):
        """Check missing heartbeat every 2 seconds and mark dead workers."""
        while self.status != STATUS_SHUTDOWN:
            with self.lock_workers:
                for worker_addr, worker in self.workers.items():
                    if worker.status != STATUS_DEAD:
                        worker_is_dead = worker.record_heartbeat_miss()
                        if worker_is_dead:
                            self._mark_worker_dead(worker_addr)
            time.sleep(2)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Manager:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
