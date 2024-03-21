"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket
from mapreduce.utils.utils import *
from collections import deque
import shutil


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        
        LOGGER.info("Manager init: host=%s port=%s pwd=%s", host, port, os.getcwd())
        

        # attributes
        self.host: str = host
        self.port: int = port

        self.status: int = STATUS_FREE

        self.workers: dict[tuple, W_info] = {}
        self.workers_free: deque = deque()

        self.num_jobs: int = 0
        self.job_queue: deque = deque()
        self.curr_job: Job = None
        


        # start
        thread_listenmessage = threading.Thread(target=self.listen_message)
        thread_heartbeat = threading.Thread(target=self.listen_heartbeat)
        thread_runjob = threading.Thread(target=self.runjob)
        thread_faulttolerance = threading.Thread(target=self.faulttolerance)
        thread_listenmessage.start()
        thread_heartbeat.start()
        thread_faulttolerance.start()
        thread_runjob.start()

        thread_listenmessage.join()
        thread_runjob.join()
        thread_heartbeat.join()
        thread_faulttolerance.join()


    def listen_message(self):
        """main TCP socket listening messages"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            LOGGER.info("manager setup TCP")

            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)

            while True:
                # try connection
                try:
                    clientsocket, address = sock.accept()
                    LOGGER.info(address)
                except socket.timeout:
                    continue
                
                # get message dict
                try:
                    message_dict = tcp_receive_json(clientsocket)
                except:
                    LOGGER.error("message_dict not succeffsulyly received")
                    continue

                try:
                    message_type = message_dict["message_type"]
                except KeyError:
                    continue
                
                # execute according function
                LOGGER.info(f"manager receive message, type {message_type}")
                if message_type == "shutdown":
                    self._shutdown(sock)
                elif message_type == "register":
                    self._register_worker(message_dict["worker_host"], int(message_dict["worker_port"]))
                elif message_type == "new_manager_job":
                    self._create_job(message_dict["input_directory"], message_dict["output_directory"], message_dict["mapper_executable"], message_dict["reducer_executable"], int(message_dict["num_mappers"]), int(message_dict["num_reducers"]),)
                elif message_type == "finished":
                    self._mark_task_finished(message_dict["task_id"], message_dict["worker_host"], message_dict["worker_port"])
                else:
                    continue

    def _shutdown(self, main_sock: socket):
        LOGGER.info(f"call shutdown on manager, now there is {len(self.workers)} workers")

        self.status = STATUS_SHUTDOWN
        for addr, worker in self.workers.items():
            if worker.status != STATUS_DEAD:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    try:
                        sock.connect((worker.host, worker.port))
                    except ConnectionRefusedError:
                        continue
                    shutdown_message = json.dumps({
                        "message_type": "shutdown"
                    })
                    sock.sendall(shutdown_message.encode('utf-8'))
        
        main_sock.close()
        os._exit(0)


    def _register_worker(self, worker_host: str, worker_port: int):
        LOGGER.info(f"register worker {worker_host}, {worker_port}")

        self.workers[(worker_host, worker_port)] = W_info(worker_host, worker_port)
        self.workers_free.append((worker_host, worker_port))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((worker_host, worker_port))
            registar_ack_message = json.dumps({"message_type": "register_ack"})
            sock.sendall(registar_ack_message.encode('utf-8'))

    
    def _create_job(self, input_directory, output_directory, mapper_executable, reducer_executable, num_mappers, num_reducers):
        new_job: Job = Job(self.num_jobs, input_directory, output_directory, mapper_executable, reducer_executable, num_mappers, num_reducers)
        self.job_queue.append(new_job)

        self.num_jobs += 1

    def _mark_task_finished(self, task_id:int, worker_host: str, worker_port:int):
        # mark the task as finished
        self.curr_job.task_list[task_id]["status"] = "finished"
        # worker back to ready
        self.workers[(worker_host, worker_port)].status = STATUS_FREE
        self.workers_free.append((worker_host, worker_port))

    def runjob(self):
        """thread: run a job"""

        while True:
            # The Manager runs each job to completion before starting a new job.
            while self.status == STATUS_BUSY or len(self.job_queue) == 0:
                time.sleep(0.1)

            # start a new job
            self.status = STATUS_BUSY
            self.curr_job: Job = self.job_queue.popleft()
            LOGGER.info(f"starting to run a job: {self.curr_job.id}")

            # delete the output directory if it already exists, create the output directory
            if os.path.exists(self.curr_job.output_directory):
                shutil.rmtree(self.curr_job.output_directory)
            output_dir = self.curr_job.output_directory
            os.makedirs(output_dir)

            prefix = f"mapreduce-shared-job{self.curr_job.id:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                LOGGER.info("Created tmpdir %s", tmpdir)

                # Sort the input files by name
                input_files = sorted(os.listdir(self.curr_job.input_directory))

                # Divide the input files into num_mappers partitions using round robin
                partitions = [[] for _ in range(self.curr_job.num_mappers)]
                for i, file in enumerate(input_files):
                    partitions[i % self.curr_job.num_mappers].append(file)

                # for every task
                for task_id, partition in enumerate(partitions):
                    # register the task on the job
                    self.curr_job.register_task_list(task_id, partition)

                    # wait if no workers available
                    while len(self.workers_free) == 0:
                        time.sleep(0.1)
                    if self.status == STATUS_SHUTDOWN:
                        break
                    
                    # get an free worker, and send it the task
                    worker_host, worker_port = self.workers_free.popleft()
                    self.workers[(worker_host, worker_port)].status = STATUS_BUSY
                    task_message = {
                        "message_type": "new_map_task",
                        "task_id": task_id,
                        "input_paths": partition,
                        "executable": self.curr_job.mapper_executable,
                        "output_directory": self.curr_job.output_directory,
                        "num_partitions": self.curr_job.num_reducers
                    }

                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        sock.connect((worker_host, worker_port))
                        message = json.dumps(task_message)
                        sock.sendall(message.encode('utf-8'))
            
                while not self.status == STATUS_SHUTDOWN or not self.curr_job.is_all_tasks_completed():
                    time.sleep(0.1)
            
            # now the current job is completed
            self.curr_job = None
            self.status = STATUS_FREE
            


                    

    def listen_heartbeat(self):
        """UDP socket listening for heartbeats"""

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.settimeout(1)
            LOGGER.info("Manager setup UDP")

            while True:
                try:
                    message_dict = udp_receive_json(sock)
                except:
                    continue
                
                if (message_dict["worker_host"], message_dict["worker_port"]) not in self.workers:
                    continue
                # LOGGER.info(f"manager UDP receive heartbeat message")


    def faulttolerance(self):
        pass

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
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
