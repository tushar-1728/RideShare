import json
import uuid
import threading
import logging
from flask import Flask, request, make_response
import pika
import docker
from kazoo.client import KazooClient
from math import ceil

app = Flask(__name__)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq', heartbeat=0)
)
channel = connection.channel()
channel.queue_declare(queue='writeQ')

logging.basicConfig()
zk = KazooClient(hosts='zoo:2181')
zk.start()

# flag denoting whether the timer has started or no
TIMER_START_FLAG = 0
# variable which stores the count of all read requests made
# gets incremented after each read request is received
REQUEST_COUNT = 0
# variable just used for naming the worker containers distinctly
# incremented before creating every new worker
WORKER_COUNT = 0
# variable keeps count of all the masters running currently, for our case it is always 1
MASTER_COUNT = 0
# variable keeps count of all the slaves running currently
SLAVE_COUNT = 0
# list storing all the slave containers
SLAVE_LIST = []
# list storing all the master containers
MASTER_LIST = []


@zk.DataWatch("/worker/slave/")
def slave_watch(data, stat):
    if data:
        global SLAVE_LIST
        global WORKER_COUNT
        data = data.decode()
        if data == "deleted":
            WORKER_COUNT += 1
            container = client.containers.run(
                "workers:latest",
                detach=True,
                name="worker_container" + str(WORKER_COUNT),
                network="orch-network",
                command=["sh", "-c", "service mongodb start; python3 worker.py 0"]
            )
            SLAVE_LIST.append(container)
            pid = p_client.inspect_container(container.name)['State']['Pid']
            message = ("running " + str(pid)).encode()
            zk.set("/worker/slave", message)
            zk.create_async("/worker/slave" + str(pid), b"running")


@zk.DataWatch("/worker/master")
def master_watch(data, stat):
    if(data):
        data = data.decode()
        if(data == "deleted"):
            global SLAVE_LIST
            global MASTER_LIST
            global WORKER_COUNT

            pid_list = []
            for i in SLAVE_LIST:
                pid = p_client.inspect_container(i.name)['State']['Pid']
                pid_list.append(pid)
            min_pid = min(pid_list)
            min_pid_index = pid_list.index(min_pid)
            container = SLAVE_LIST.pop(min_pid_index)
            MASTER_LIST.append(container)

            zk.delete_async("/worker/slave/" + str(min_pid))
            zk.set("/worker/slave", b"deleted")
            zk.create_async("/worker/master/" + str(min_pid), b"running")

            params = {"_id":-1, "data": {"func": "change_designation", "pid": str(min_pid)}}
            params = json.dumps(params).encode()
            channel.basic_publish(
                exchange="syncQ",
                routing_key="",
                body=params
            )


class RpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq', heartbeat=0)
        )

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='responseQ')
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response
        )

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.response = body

    def read_call(self, params):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='readQ',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=params
        )
        while self.response is None:
            self.connection.process_data_events()
        return self.response


rpc_client = RpcClient()


def write_call(params):
    channel.basic_publish(
        exchange="",
        routing_key="writeQ",
        body=params
    )


def timer_func():
    global REQUEST_COUNT
    global SLAVE_COUNT
    global SLAVE_LIST
    global WORKER_COUNT
    req_SLAVE_COUNT = ceil(REQUEST_COUNT / 20)
    print("request count:", REQUEST_COUNT)
    print("required slave count:", req_SLAVE_COUNT)
    while(req_SLAVE_COUNT > SLAVE_COUNT):
        SLAVE_COUNT += 1
        WORKER_COUNT += 1
        container = client.containers.run(
            "workers:latest",
            detach=True,
            name="worker_container" + str(WORKER_COUNT),
            network="orch-network",
            command=["sh", "-c", "service mongodb start; python3 worker.py 0"]
        )
        SLAVE_LIST.append(container)
        pid = p_client.inspect_container(container.name)['State']['Pid']
        message = ("running " + str(pid)).encode()
        zk.set("/worker/slave", message)
        zk.create_async("/worker/slave" + str(pid), b"running")
    while(req_SLAVE_COUNT < SLAVE_COUNT and SLAVE_COUNT > 1):
        SLAVE_COUNT -= 1
        container = SLAVE_LIST.pop()
        pid = p_client.inspect_container(container.name)['State']['Pid']
        container.stop(timeout=0)
        container.remove()
        zk.delete_async("/worker/slave/" + str(pid))

    REQUEST_COUNT = 0
    print("\n\ntimer restarted\n\n")
    timer = threading.Timer(2 * 60, timer_func)
    timer.start()


# api 8
@app.route('/api/v1/db/read', methods=['GET'])
def db_read():
    global REQUEST_COUNT
    global TIMER_START_FLAG

    # read request count gets incremented by 1 after every read request received
    REQUEST_COUNT += 1

    if(TIMER_START_FLAG == 0):
        # flag set to 1 denoting timer has started
        TIMER_START_FLAG = 1
        # timer is set to 2 mins and after that timer_func is called
        timer = threading.Timer(2 * 60, timer_func)
        print("\n\ntimer func started\n\n")
        # starts the timer
        timer.start()

    # the origin argument in the request body denotes which microservice called the db read api
    # if condition segregating all db read apis coming from Rides microservice
    if request.args.get('ORIGIN') == "RIDE":
        if request.args.get('COMMAND') == "Upcoming":
            source = request.args.get('source')
            destination = request.args.get('destination')
            params = "get_upcoming_rides:" + str(source) + "," + str(destination)
            message = rpc_client.read_call(params)
            message = json.loads(message.decode())["message"]
            if message == []:
                return make_response('', 204)
            return json.dumps({'upcoming': message}), 200

        if request.args.get('COMMAND') == "EXISTS":
            collection_name = request.args.get("COLLECTION")
            val = request.args.get('VALUE')
            field = str(request.args.get("FIELD"))
            params = "entry_exists:" + str(collection_name) + "," + str(field) + "," + str(val)
            message = rpc_client.read_call(params).decode()
            return message, 200

        if request.args.get('COMMAND') == "Ride_Details":
            id = request.args.get('id')
            message = rpc_client.read_call("get_ride_details:" + str(id)).decode()
            return message, 200

        if request.args.get('COMMAND') == "READ_REQUEST_COUNT":
            message = rpc_client.read_call("read_request_count_ride:").decode()
            return message, 200

        if request.args.get('COMMAND') == "READ_RIDE_COUNT":
            message = rpc_client.read_call("read_ride_count:").decode()
            return message, 200

    # if condition segregating all db read apis coming from Users microservice
    elif request.args.get('ORIGIN') == "USER":
        if request.args.get('COMMAND') == "EXISTS":
            collection_name = request.args.get("DB")
            val = request.args.get('VALUE')
            field = str(request.args.get("FIELD"))
            params = "entry_exists:" + str(collection_name) + "," + str(field) + "," + str(val)
            message = rpc_client.read_call(params).decode()
            return message, 200

        if request.args.get('COMMAND') == "READ_ALL":
            message = rpc_client.read_call("read_all_users:").decode()
            if (message == "0"):
                return make_response('', 204)
            else:
                return message, 200

        if request.args.get('COMMAND') == "READ_REQUEST_COUNT":
            message = rpc_client.read_call("read_request_count_user:").decode()
            return message, 200


# api 9
@app.route('/api/v1/db/write', methods=['POST'])
def db_write():
    req = json.loads(request.data)
    # rides-start
    if (req['ORIGIN'] == 'RIDE'):
        if (req['COMMAND'] == 'INSERT'):
            collection_name = req['COLLECTION']
            fields = req['FIELDS']
            data = {}
            for field in range(len(fields)):
                data[fields[field]] = req["VALUES"][field]
            params = {"func": "create_entry", "collection": collection_name, "data": data}
            params = json.dumps(params).encode()
            write_call(params)
            return make_response("", 201)

        if(req['COMMAND'] == 'DELETE'):
            collection_name = req['COLLECTION']
            data = {
                req['FIELD']: req['VALUE']
            }
            params = {"func": "delete_entry", "collection": collection_name, "data": data}
            params = json.dumps(params).encode()
            write_call(params)
            return make_response("", 201)

        if(req['COMMAND'] == 'Update_Ride'):
            username = req['username']
            id = int(req['id'])
            params = json.dumps({"func": "update_ride", "username": username, "id": id}).encode()
            write_call(params)
            return make_response("", 200)

        if (req['COMMAND'] == "DELETE_ALL"):
            params = json.dumps({"func": "delete_all", "collection": "Rides"}).encode()
            write_call(params)
            return make_response("", 200)

        if (req['COMMAND'] == "RESET_REQUEST_COUNT"):
            params = json.dumps({"func": "reset_request_count_ride"}).encode()
            write_call(params)
            return make_response("", 200)

        if (req['COMMAND'] == "ADD_REQUEST_COUNT"):
            params = json.dumps({"func": "add_request_count_ride"}).encode()
            write_call(params)
            return make_response("", 200)

        if (req['COMMAND'] == "ADD_RIDE_COUNT"):
            params = json.dumps({"func": "add_ride_count"}).encode()
            write_call(params)
            return make_response("", 200)

    if (req["ORIGIN"] == "USER"):
        if (req['COMMAND'] == 'INSERT'):
            collection = req['DB']
            fields = req['FIELDS']
            data = {}
            for field in range(len(fields)):
                data[fields[field]] = req["VALUES"][field]
            params = {"func": "create_entry", "collection": collection, "data": data}
            params = json.dumps(params).encode()
            write_call(params)
            return make_response("", 201)

        if(req['COMMAND'] == 'DELETE'):
            collection = req['DB']
            data = {
                req['FIELD']: req['VALUE']
            }
            params = {"func": "delete_entry", "collection": collection, "data": data}
            params = json.dumps(params).encode()
            write_call(params)
            return make_response("", 200)

        if (req['COMMAND'] == "DELETE_ALL"):
            params = json.dumps({"func": "delete_all", "collection": "Users"}).encode()
            write_call(params)
            return make_response("", 200)

        if (req['COMMAND'] == "RESET_REQUEST_COUNT"):
            params = json.dumps({"func": "reset_request_count_user"}).encode()
            write_call(params)
            return make_response("", 200)

        if (req['COMMAND'] == "ADD_REQUEST_COUNT"):
            params = json.dumps({"func": "add_request_count_user"}).encode()
            write_call(params)
            return make_response("", 200)


# api list workers
@app.route('/api/v1/worker/list', methods=['GET'])
def get_worker_list():
    pid_list = []
    for i in MASTER_LIST:
        pid_list.append(p_client.inspect_container(i.name)['State']['Pid'])

    for i in SLAVE_LIST:
        pid_list.append(p_client.inspect_container(i.name)['State']['Pid'])

    pid_list.sort()
    return make_response(str(pid_list), 200)


# api crash slave
@app.route('/api/v1/crash/slave', methods=['POST'])
def crash_slave():
    pid_list = []
    for i in SLAVE_LIST:
        pid_list.append(p_client.inspect_container(i.name)['State']['Pid'])
    max_pid = max(pid_list)
    max_pid_index = pid_list.index(max_pid)
    container = SLAVE_LIST.pop(max_pid_index)
    container.stop(timeout=10)
    params = {"_id":-1, "data": {"func": "stop_consuming", "pid": str(max_pid)}}
    params = json.dumps(params).encode()
    channel.basic_publish(
        exchange="syncQ",
        routing_key="",
        body=params
    )
    container.remove()
    zk.delete("/worker/slave/" + str(max_pid))
    zk.set("/worker/slave", b"deleted")
    return make_response(str(max_pid), 200)


# api crash master
@app.route('/api/v1/crash/master', methods=['POST'])
def crash_master():
    container = MASTER_LIST.pop()
    pid = p_client.inspect_container(container.name)['State']['Pid']
    container.stop(timeout=0)
    container.remove()
    zk.delete("/worker/master/" + str(pid))
    zk.set("/worker/master", b"deleted")
    return make_response(str(pid), 200)


if __name__ == '__main__':
    # 
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    p_client = docker.APIClient(base_url='unix://var/run/docker.sock')

    channel.exchange_declare(exchange='syncQ', exchange_type='fanout')
    channel.queue_declare(queue="writeQ")
    channel.queue_declare(queue='readQ')

    MASTER_COUNT += 1
    SLAVE_COUNT += 1

    WORKER_COUNT += 1
    container = client.containers.run(
        "workers:latest",
        detach=True,
        name="worker_container" + str(WORKER_COUNT),
        network="orch-network",
        command=["sh", "-c", "service mongodb start; python3 worker.py 1"]
    )
    pid = p_client.inspect_container(container.name)['State']['Pid']
    MASTER_LIST.append(container)
    message = ("running " + str(pid)).encode()
    zk.create("/worker/master", message, makepath=True)
    zk.create("/worker/master/" + str(pid), b"running")

    WORKER_COUNT += 1
    container = client.containers.run(
        "workers:latest",
        detach=True,
        name="worker_container" + str(WORKER_COUNT),
        network="orch-network",
        command=["sh", "-c", "service mongodb start; python3 worker.py 0"]
    )
    pid = p_client.inspect_container(container.name)['State']['Pid']
    SLAVE_LIST.append(container)
    message = ("running " + str(pid)).encode()
    zk.create_async("/worker/slave", message, makepath=True)
    zk.create_async("/worker/slave/" + str(pid), b"running")

    app.run(host='0.0.0.0', debug=False)
