from graphviz import Digraph

# Create a UML-style class diagram for the given code
dot = Digraph(comment="UML Class Diagram")

# Caller class
dot.node("Caller", '''Caller
-----------------
+ name: str''')

# CommsHandlerABC class
dot.node("CommsHandlerABC", '''CommsHandlerABC (abstract)
---------------------------
+ connect(user1: Caller, user2: Caller): str
+ hangup(user1: Caller, user2: Caller): str
+ clear_all(): None''')

# ConnectionException class
dot.node("ConnectionException", '''ConnectionException
--------------------------------
+ message: str''')

# CommsHandler class
dot.node("CommsHandler", '''CommsHandler
-------------------------
- active_connections: list
+ connect(user1: Caller, user2: Caller): str
+ hangup(user1: Caller, user2: Caller): str
+ clear_all(): None''')

# Relationships
dot.edge("CommsHandler", "CommsHandlerABC", label="inherits", arrowhead="onormal")
dot.edge("CommsHandler", "Caller", label="uses", arrowhead="vee")
dot.edge("ConnectionException", "Exception", label="inherits", arrowhead="onormal")

dot.render("/home/akshay/Documents/Setups/RetailAnalysis/RetailAnalysis/misc/Python/uml", format="png", cleanup=True)
