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
from utils import *
from collections import deque
import shutil


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # attributes
        self.host: str = host
        self.port: int = port

        self.status: int = STATUS_FREE

        self.num_workers: int = 0
        self.workers: dict[int, W_info] = {}

        self.num_jobs: int = 0
        self.job_queue: deque = deque()


        # start
        thread_heartbeat = threading.Thread(target=self.listen_heartbeat)
        thread_runjob = threading.Thread(target=self.runjob)
        thread_faulttolerance = threading.Thread(target=self.faulttolerance)
        thread_heartbeat.start()
        thread_faulttolerance.start()
        thread_runjob.start()

        self.listen_message()

        thread_heartbeat.join()
        thread_faulttolerance.join()


    def listen_message(self):
        """main TCP socket listening messages"""

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)

            while True:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
            
                try:
                    message_dict = tcp_receive_json(clientsocket)
                except json.JSONDecodeError:
                    continue

                try:
                    message_type = message_dict["message_type"]
                except KeyError:
                    continue

                if message_type == "shutdown":
                    self.shutdown(sock)
                elif message_type == "register":
                    self.register_worker(message_dict["worker_host"], int(message_dict["worker_port"]))
                elif message_type == "new_manager_job":
                    self.create_job(message_dict["input_directory"], message_dict["output_directory"], message_dict["mapper_executable"], message_dict["reducer_executable"], int(message_dict["num_mappers"]), int(message_dict["num_reducers"]),)
                else:
                    continue

    def shutdown(self, main_sock: socket):
        for worker_id, worker in self.workers.items():
            if worker.status != STATUS_DEAD:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((worker.host, worker.port))
                    shutdown_message = json.dumps({
                        "message_type": "shutdown"
                    })
                    sock.sendall(shutdown_message.encode('utf-8'))
        
        main_sock.close()
        os._exit(0)

    def register_worker(self, worker_host: str, worker_port: int):
        self.workers[self.num_workers] = W_info(worker_host, worker_port, self.num_workers)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((worker_host, worker_port))
            registar_ack_message = json.dumps({"message_type": "register_ack"})
            sock.sendall(registar_ack_message.encode('utf-8'))

        self.num_workers += 1

        if self.num_workers == 1:
            pass
            # FIXME: After the first Worker registers with the Manager, the Manager should check the job queue (described later) if it has any work it can assign to the Worker (because a job could have arrived at the Manager before any Workers registered). If the Manager is already in the middle of a job, it should assign the Worker the next available task immediately.

    
    def create_job(self, input_directory, output_directory, mapper_executable, reducer_executable, num_mappers, num_reducers):
        new_job: Job = Job(self.num_jobs, input_directory, output_directory, mapper_executable, reducer_executable, num_mappers, num_reducers)
        self.job_queue.append(new_job)

        self.num_jobs += 1

    def runjob(self):

        while True:
            # The Manager runs each job to completion before starting a new job.
            while self.status == STATUS_BUSY or len(self.job_queue) == 0:
                time.sleep(0.1)
        
            job: Job = self.job_queue.popleft()
            self.curr_job_id = job.id

            # delete the output directory if it already exists
            if os.path.exists(job.output_directory):
                shutil.rmtree(job.output_directory)
            
            # create the output directory
            output_dir = job.output_directory
            os.makedirs(output_dir)

            prefix = f"mapreduce-shared-job{job.id:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                LOGGER.info("Created tmpdir %s", tmpdir)
                # FIXME: Change this loop so that it runs either until shutdown 
                # or when the job is completed.

                # Sort the input files by name
                input_files = sorted(os.listdir(job.input_directory))

                # Divide the input files into num_mappers partitions using round robin
                partitions = [[] for _ in range(job.num_mappers)]
                for i, file in enumerate(input_files):
                    partitions[i % job.num_mappers].append(file)

                i = 0
                while i < len(partitions):
                    for worker_id, worker in self.workers.items():
                        if worker.status == STATUS_READY:
                            task_message = {
                                "message_type": "new_map_task",
                                "task_id": task_id,
                                "input_paths": partitions[i],
                                "executable": job.mapper_executable,
                                "output_directory": job.output_directory,
                                "num_partitions": job.num_reducers
                            }

                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                sock.connect((worker.host, worker.port))
                                message = json.dumps(task_message)
                                sock.sendall(message.encode('utf-8'))
                            
                            i += 1
                    
                    # FIXME: if all full, what to do? how to avoid busy waiting

                    
                    
                    
                
            LOGGER.info("Cleaned up tmpdir %s", tmpdir)
                    

    def listen_heartbeat(self):
        """UDP socket listening for heartbeats"""

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.settimeout(1)

            while True:
                try:
                    message_dict = udp_receive_json(socket)
                except socket.timeout or json.JSONDecodeError:
                    continue

                

        pass

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
