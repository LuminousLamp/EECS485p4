"""Example TCP socket client."""
import socket
import json


def main():
    """Test TCP Socket Client."""
    # create an INET, STREAMing socket, this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

        # connect to the manager
        sock.connect(("localhost", 6000))

        # send a message
        message = json.dumps({
            "message_type": "new_manager_job",
            "input_directory": "tests/testdata/input_small",
            "output_directory": "output/",
            "mapper_executable": "tests/testdata/exec/wc_map.py",
            "reducer_executable": "tests/testdata/exec/wc_reduce.py",
            "num_mappers" : 2,
            "num_reducers" : 2
            })
        sock.sendall(message.encode('utf-8'))


if __name__ == "__main__":
    main()
