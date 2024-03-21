"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import socket
from utils import *
import threading


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register_ack",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        
        self.host: str = host
        self.port: int = port
        self.manager_host: str = manager_host
        self.manager_port: int = manager_port
        self.status: int = STATUS_READY

        self.listen_message()
        

    def listen_message(self):
        """main TCP socket listening messages"""

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))

            # Once a Worker is ready to listen for instructions, it should send a message
            self.register()
            sock.listen()
            sock.settimeout(1)

            while True:
                try:
                    clientsocket, address = sock.accept()
                except sock.timeout:
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
                elif message_type == "register_ack":
                    thread_sendheartbeat = threading.Thread(target=self.send_heartbeat)
                    self.send_heartbeat()



    def register(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            register_message = json.dumps({
                "message_type" : "register",
                "worker_host" : self.host,
                "worker_port" : self.port
            })

            sock.sendall(register_message.encode('utf-8'))

    def shutdown(self, main_sock: socket):
        while self.status == STATUS_BUSY:
            time.sleep(0.1)
        
        main_sock.close()
        os._exit(0)

    def send_heartbeat():
        pass



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
