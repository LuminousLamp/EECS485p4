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

        self.task_id: int = None
        self.input_paths: list[str] = None
        self.executable: str = None
        self.output_directory: str = None
        self.num_partitions: int = None

        self.signals = {
            "registered": False
        }

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
                elif message_type == "new_map_task" or message_type == "new_reduce_task":
                    assert (self.status == STATUS_READY, 
                        "the worker should be STATUS_READY upon taking a task, but it is not")
                    self.task_id = message_dict["task_id"]
                    self.input_paths = message_dict["input_paths"]
                    self.executable = message_dict["executable"]
                    self.output_directory = message_dict["output_directory"]
                    if message_type == "new_map_task":
                        self.num_partitions = int(message_dict["num_partitions"])
                        self.do_maptask() # TODO: can we just use the main thread?
                        # i.e. can we not listening to any signal during map time?
                    elif message_type == "new_reduce_task":
                        self.do_reducetask()





    def _register(self):
        LOGGER.info("worker register")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            register_message = json.dumps({
                "message_type" : "register",
                "worker_host" : self.host,
                "worker_port" : self.port
            })

            sock.sendall(register_message.encode('utf-8'))

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
                heartbeat_message = json.dumps({
                    "message_type": "heartbeat",
                    "worker_host": self.host,
                    "worker_port": self.port
                })
                sock.sendall(heartbeat_message.encode('utf-8'))
                time.sleep(1)

    def do_maptask(self):
        
        LOGGER.info(f"worker begin to do map task {self.task_id}")
        self.status = STATUS_BUSY
        
        prefix = f"mapreduce-local-task{self.task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            # generate intermediate files
            outfiles: list[TextIOWrapper] = []
            for i in range(self.num_partitions):
                    filename = os.path.join(tmpdir, f"maptask{self.task_id:05d}-part{i:05d}")
                    outfile = open(filename, "w")
                    outfiles.append(outfile)

            # Run the map executable on all the input files
            # for every input file
            for input_path in self.input_paths:
                with open(input_path) as infile:
                    with subprocess.Popen(
                        [self.executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        # for every output line
                        for line in map_process.stdout:
                            key, value = line.split('\t')
                            hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
                            keyhash = int(hexdigest, base=16)
                            partition_number = keyhash % self.num_partitions
                            outfiles[partition_number].write(f"{key}\t{value}\n")

            # sort intermediate files
            for i in range(self.num_partitions):
                filename = os.path.join(tmpdir, f"maptask{self.task_id:05d}-part{i:05d}")
                LOGGER.info(f"worker is going to sort the file {filename}")
                subprocess.run(["sort", "-o", filename, filename], check=True)

            # Move the sorted output files into the output_directory specified by the task
            for i in range(self.num_partitions):
                filename = os.path.join(tmpdir, f"maptask{self.task_id:05d}-part{i:05d}")
                output_filename = os.path.join(self.output_directory, f"maptask{self.task_id:05d}-part{i:05d}")
                os.rename(filename, output_filename)

        # task finished, send a TCP message to the Manager’s main socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps({
                "message_type": "finished",
                "task_id": self.task_id,
                "worker_host": self.host,
                "worker_port": self.port
            })
            sock.sendall(message.encode("utf-8"))

        self.status = STATUS_READY
    
    def do_reducetask(self):
        LOGGER.info(f"worker begin to do map task {self.task_id}")
        self.status = STATUS_BUSY



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
