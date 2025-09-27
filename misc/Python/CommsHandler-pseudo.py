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

#Enter your code here.
'''
Templates for messages. Use these for copy/paste to avoid typing.

{user1.name} cannot connect with {user2.name}
Connection in use. Please try later
Connection established between {user1.name} and {user2.name}
{user1.name} cannot hangup with {user2.name}
{user1.name} and {user2.name} not found in the communication channel
{user1.name} and {user2.name} are disconnected
'''

class ConnectionException(Exception):
    '''Implemen this exception class'''
    
class CommsHandler(CommsHandlerABC):
    '''implement this class'''
    connected = []
    connect(u1,u2):
        if u1 not in connected and u2 not in connected:
            connected.append(u1)
            connected.append(u2)
        else:
            raise ConnectionException()
            
    
    hangup(u1,u2):
        if u1 in connected and u2 in connected:
            connected.append(u1)
            connected.append(u2)
        else:
            raise ConnectionException()
    
    

def main(path="/dev/stdout") -> None:
    # Create Communication
    comms = CommsHandler()
    functions = {"connect": comms.connect, "hangup": comms.hangup}
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
        u1, u2 = map(int, instructions[1:])
        try:
            result_str += (
                "Success: " + functions[instructions[0]](users[u1], users[u2]) + "\n"
            )
        except ConnectionException as ce:
            result_str += "Error: " + str(ce) + "\n"
    comms.clear_all()
    # assert len(comms.active_connections) == 0

    with open(path, "w") as fptr:
        fptr.write(result_str)


if __name__ == "__main__":
    path = "/dev/stdout"
    if "OUTPUT_PATH" in os.environ.keys():
        path = os.environ["OUTPUT_PATH"]
    main(path=path)