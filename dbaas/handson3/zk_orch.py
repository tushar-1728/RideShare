"""
Kazoo is the python binding for zookeeper, for students planning to use node, you can look into,
"node-zookeeper-client" https://github.com/alexguan/node-zookeeper-client
For the purpose of the demo, we are doing everything synchronously.
You need to choose based on the use case, if you need nodes to be created/deleted etc synchronously or asynchronously.

For most things, async is the way to go.
https://kazoo.readthedocs.io/en/latest/async_usage.html
"""
import logging

import json
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType

"""
Notice, the znodes we are creating are not linked to any process here.
For the project, this code has to be present in the workers itself, where a znode has to be created for each worker, 
and when that worker is killed, because of the watch on that znode, a new leader has to be elected appropriately.
For this, you will also have to add appropriate listeners to handle the different connection states, 
eg create a znode when a worker is created and connects to zookeeper for the first time.
https://kazoo.readthedocs.io/en/latest/basic_usage.html#listening-for-connection-events
"""

logging.basicConfig()

# Can also use the @DataWatch and @ChildrenWatch decorators for the same
def demo_func(event):
    # Create a node with data
    zk.create("/producer/node_2", b"new demo producer node")
    print("apple")
    # print(dir(event))
    print(event)
    # print(event.state)
    # print(event.path)
    # print(event.type)
    children = zk.get_children("/producer")
    print("There are %s children with names %s" % (len(children), children))

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
# Deleting all existing nodes (This is just for the demo to be consistent)
zk.delete("/producer", recursive=True)

# Ensure a path, create if necessary
zk.ensure_path("/producer")

# Create a node with data
if zk.exists("/producer/node_1"):
    print("Node already exists")
else:
    zk.create("/producer/node_1", b"demo producer node")

# @zk.DataWatch("/producer/node_1")
# def watch(data, stat):
#     if(data):
#         data = data.decode()
#         print(data)
#         print(stat)
#         print("data watch working")
#     else:
#         print("node deleted")

@zk.ChildrenWatch("/producer")
def child_watch(children):
    print(children)
    print("child watch working")

zk.set("/producer/node_1", json.dumps({"app":"fruit"}).encode())

# Print the version of a node and its data
data, stat = zk.get("/producer/node_1")
print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

# List the children
children = zk.get_children("/producer")#, watch=demo_func)
print("There are %s children with names %s" % (len(children), children))

zk.delete("/producer/node_1")
print("Deleted /producer/node_1")

zk.create("/producer/node_5", b"demo producer node")
zk.delete("/producer/node_5")