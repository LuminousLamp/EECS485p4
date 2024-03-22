"""MapReduce framework Worker node."""

import os
import logging
import json
import time
import click
import socket
from mapreduce.utils.utils import *
import threading
import subprocess
import tempfile
import hashlib
from io import TextIOWrapper
import heapq


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""

        LOGGER.info("Worker init: host=%s port=%s pwd=%s", host, port, os.getcwd())

        # member variables
        self.host: str = host
        self.port: int = port
        self.manager_host: str = manager_host
        self.manager_port: int = manager_port

        self.status: int = STATUS_READY

        # self.task_id: int = None
        # self.input_paths: list[str] = None
        # self.executable: str = None
        # self.output_directory: str = None
        # self.num_partitions: int = None

        self.signals = {"registered": False}

        # start
        thread_listenmessage = threading.Thread(target=self.listen_message)
        thread_sendheartbeat = threading.Thread(target=self.send_heartbeat)
        thread_listenmessage.start()
        thread_sendheartbeat.start()
        thread_listenmessage.join()
        thread_sendheartbeat.join()

    def listen_message(self):
        """main TCP socket listening messages"""

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            LOGGER.info("worker setup TCP")
            sock.listen()
            # Once a Worker is ready to listen for instructions, it should send a message
            self._register()
            sock.settimeout(1)

            while True:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue

                try:
                    message_dict = tcp_receive_json(clientsocket)
                except:
                    LOGGER.error("message_dict not succeffsulyly received")
                    continue

                try:
                    message_type = message_dict["message_type"]
                except KeyError:
                    continue

                LOGGER.info(f"worker receive massage, type {message_type}")
                if message_type == "shutdown":
                    self._shutdown(sock)
                elif message_type == "register_ack":
                    self.signals["registered"] = True
                elif message_type == "new_map_task":
                    self._do_maptask(
                        int(message_dict["task_id"]),
                        message_dict["input_paths"],
                        message_dict["executable"],
                        message_dict["output_directory"],
                        int(message_dict["num_partitions"]),
                    )
                elif message_type == "new_reduce_task":
                    self._do_reducetask(
                        int(message_dict["task_id"]),
                        message_dict["input_paths"],
                        message_dict["executable"],
                        message_dict["output_directory"],
                    )
                else:
                    continue

    def _register(self):
        LOGGER.info("worker register")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            register_message = json.dumps(
                {
                    "message_type": "register",
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
            )

            sock.sendall(register_message.encode("utf-8"))

    def _shutdown(self, main_sock: socket):
        LOGGER.info("worker shutdown")
        while self.status == STATUS_BUSY:
            time.sleep(0.1)
        self.status = STATUS_SHUTDOWN
        main_sock.close()
        os._exit(0)

    def send_heartbeat(self):
        while not self.signals["registered"]:
            time.sleep(0.1)

        LOGGER.info("worker starts to send heartbeat")
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            while True:
                sock.connect((self.manager_host, self.manager_port))
                heartbeat_message = json.dumps(
                    {
                        "message_type": "heartbeat",
                        "worker_host": self.host,
                        "worker_port": self.port,
                    }
                )
                sock.sendall(heartbeat_message.encode("utf-8"))

                # send it every 2 seconds
                time.sleep(2)

    def _do_maptask(self, task_id: int, input_paths: list[str], executable: str, output_directory: str, num_partitions: int):
        assert (
            self.status == STATUS_READY
        ), "the worker should be STATUS_READY upon taking a task, but it is not"
        LOGGER.info(f"worker begin to do map task {task_id}")
        self.status = STATUS_BUSY

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            # generate intermediate files
            outfiles: list[TextIOWrapper] = []
            for i in range(num_partitions):
                filename = os.path.join(
                    tmpdir, f"maptask{task_id:05d}-part{i:05d}"
                )
                outfile = open(filename, "w")
                outfiles.append(outfile)

            # Run the map executable on all the input files
            # for every input file
            for input_path in input_paths:
                with open(input_path) as infile:
                    with subprocess.Popen(
                        [executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        # for every output line
                        for line in map_process.stdout:
                            key, value = line.rstrip().split("\t")
                            hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
                            keyhash = int(hexdigest, base=16)
                            partition_number = keyhash % num_partitions
                            outfiles[partition_number].write(f"{key}\t{value}\n")
            for outfile in outfiles:
                outfile.close()

            # sort intermediate files
            for i in range(num_partitions):
                filename = os.path.join(
                    tmpdir, f"maptask{task_id:05d}-part{i:05d}"
                )
                LOGGER.info(f"worker is going to sort the file {filename}")
                subprocess.run(["sort", "-o", filename, filename], check=True)

            # Move the sorted output files into the output_directory specified by the task
            for i in range(num_partitions):
                filename = os.path.join(
                    tmpdir, f"maptask{task_id:05d}-part{i:05d}"
                )
                output_filename = os.path.join(
                    output_directory, f"maptask{task_id:05d}-part{i:05d}"
                )
                os.rename(filename, output_filename)

        # task finished, send a TCP message to the Manager’s main socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps(
                {
                    "message_type": "finished",
                    "task_id": task_id,
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
            )
            sock.sendall(message.encode("utf-8"))

        self.status = STATUS_READY

    def _do_reducetask(self, task_id: int, input_paths: list[str], executable: str, output_directory: str):
        assert (
            self.status == STATUS_READY
        ), "the worker should be STATUS_READY upon taking a task, but it is not"
        LOGGER.info(
            f"worker begin to do reduce task {task_id}, there are {input_paths} input files"
        )
        self.status = STATUS_BUSY

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:

            input_files: list[TextIOWrapper] = [open(file) for file in input_paths]

            # Run the reduce executable on the merged input
            output_file = os.path.join(tmpdir, f"part-{task_id:05d}")
            with open(output_file, "w") as outfile:
                with subprocess.Popen(
                    [executable],
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                    text=True,
                ) as reduce_process:
                    for line in heapq.merge(*input_files):
                        reduce_process.stdin.write(line)

            # Move the output file to the output_directory specified by the task
            output_filename = os.path.join(
                output_directory, f"part-{task_id:05d}"
            )
            os.rename(output_file, output_filename)

        # task finished, send a TCP message to the Manager’s main socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps(
                {
                    "message_type": "finished",
                    "task_id": task_id,
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
            )
            sock.sendall(message.encode("utf-8"))

        self.status = STATUS_READY


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
