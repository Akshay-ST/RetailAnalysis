import os
from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass
class Caller(object):
    name: str


class CommsHandlerABC(ABC):
    @abstractmethod
    def connect(self, user1: Caller, user2: Caller) -> str:
        """implement connect method"""

    @abstractmethod
    def hangup(self, user1: Caller, user2: Caller) -> str:
        """implement hangup method"""

    @abstractmethod
    def clear_all(self) -> None:
        """implement clear_all"""


class ConnectionException(Exception):
    """Custom exception for connection errors"""
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message


class CommsHandler(CommsHandlerABC):
    def __init__(self):
        # Store active connections as list of tuples (user1, user2)
        self.active_connections = []

    def connect(self, user1: Caller, user2: Caller) -> str:
        # Prevent self-connection
        if user1 == user2:
            raise ConnectionException(f"{user1.name} cannot connect with {user2.name}")

        # Check if either user is already in any active connection
        for u1, u2 in self.active_connections:
            if user1 in (u1, u2) or user2 in (u1, u2):
                raise ConnectionException("Connection in use. Please try later")

        # Establish connection
        self.active_connections.append((user1, user2))
        return f"Connection established between {user1.name} and {user2.name}"

    def hangup(self, user1: Caller, user2: Caller) -> str:
        if user1 == user2:
            raise ConnectionException(f"{user1.name} cannot hangup with {user2.name}")

        # Check if connection exists
        if (user1, user2) in self.active_connections or (user2, user1) in self.active_connections:
            self.active_connections = [
                conn for conn in self.active_connections
                if conn != (user1, user2) and conn != (user2, user1)
            ]
            return f"{user1.name} and {user2.name} are disconnected"
        else:
            raise ConnectionException(f"{user1.name} and {user2.name} not found in the communication channel")

    def clear_all(self) -> None:
        self.active_connections.clear()


def main(path="/dev/stdout") -> None:
    # Create Communication handler
    comms = CommsHandler()
    functions = {"Connect": comms.connect, "hangup": comms.hangup}

    # Create users
    n = int(input().strip())
    users = []
    for i in range(n):
        name = input().strip()
        u = Caller(name)
        assert u.name == name
        users.append(u)

    result_str = ""
    # Perform operations
    instructions_count = int(input().strip())
    for i in range(instructions_count):
        instructions = input().strip().split()
        cmd, u1, u2 = instructions[0], int(instructions[1]) - 1, int(instructions[2]) - 1
        try:
            result_str += (
                "Success: " + functions[cmd](users[u1], users[u2]) + "\n"
            )
        except ConnectionException as ce:
            result_str += "Error: " + str(ce) + "\n"

    comms.clear_all()

    with open(path, "w") as fptr:
        fptr.write(result_str)


if __name__ == "__main__":
    path = "/dev/stdout"
    if "OUTPUT_PATH" in os.environ.keys():
        path = os.environ["OUTPUT_PATH"]
    main(path=path)
