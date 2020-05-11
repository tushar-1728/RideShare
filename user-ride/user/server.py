from flask import Flask, request, jsonify, make_response
import requests
import json


app = Flask(__name__)


def is_sha1(maybe_sha):
    if len(maybe_sha) != 40:
        return False
    try:
        int(maybe_sha, 16)
    except:
        return False
    return True


# api 1
@app.route('/api/v1/users', methods=['PUT'])
def adduser():
    requests.post(
        "http://34.238.33.155:80/api/v1/db/write",
        data=json.dumps({
            "ORIGIN": "USER",
            "COMMAND": "ADD_REQUEST_COUNT"
        })
    )
    req = request.get_json()
    if req is None:
        return make_response('Request body is empty.', 400)
    if 'username' not in req or 'password' not in req:
        return make_response('Username or password is not present in request body.', 400)
    username = req["username"]
    password = req["password"]
    if username == '' or username is None or password is None or password == '':
        return make_response('Username or password is empty.', 400)

    if (not is_sha1(password)):
        return make_response('Password is not as per given norms.', 400)

    if (requests.get(
        "http://34.238.33.155:80/api/v1/db/read",
        params={
            "ORIGIN": "USER",
            "COMMAND": "EXISTS",
            "FIELD": "username",
            "VALUE": username,
            "DB": "Users"
        }
    )).json()["count"] != 0:
        return make_response('User already exists.', 400)

    requests.post(
        "http://34.238.33.155:80/api/v1/db/write",
        data=json.dumps({
            "ORIGIN": "USER",
            "COMMAND": "INSERT",
            "FIELDS": ["username", "password"],
            "VALUES": [username, password],
            "DB": "Users"
        })
    )

    return make_response('Created user.', 201)


# api 2
@app.route('/api/v1/users/<username>', methods=['DELETE'])
def deleteuser(username):
    requests.post(
        "http://34.238.33.155:80/api/v1/db/write",
        data=json.dumps({
            "ORIGIN": "USER",
            "COMMAND": "ADD_REQUEST_COUNT"
        })
    )
    if username == '' or username is None:
        return make_response('Usename is empty.', 400)

    if (requests.get(
        "http://34.238.33.155:80/api/v1/db/read",
        params={
            "ORIGIN": "USER",
            "COMMAND": "EXISTS",
            "FIELD": "username",
            "VALUE": username,
            "DB": "Users"
        }
    )).json()["count"] == 0:
        return make_response('User does not exist.', 400)

    requests.post(
        "http://34.238.33.155:80/api/v1/db/write",
        data=json.dumps({
            "ORIGIN": "USER",
            "COMMAND": "DELETE",
            "FIELD": "username",
            "VALUE": username,
            "DB": "Users"
        })
    )

    return make_response('Deleted user.', 200)


# api 10
@app.route('/api/v1/users', methods=['GET'])
def read_all():
    requests.post(
        "http://34.238.33.155:80/api/v1/db/write",
        data=json.dumps({
            "ORIGIN": "USER",
            "COMMAND": "ADD_REQUEST_COUNT"
        })
    )
    msg = requests.get(
        "http://34.238.33.155:80/api/v1/db/read",
        params={
            "ORIGIN": "USER",
            "COMMAND": "READ_ALL",
            "DB": "Users"
        }
    )
    if(msg.status_code == 204):
        return make_response('', 204)
    elif(msg.status_code == 200):
        return make_response(jsonify(msg.json()['readall']), 200)


# api 11
@app.route('/api/v1/db/clear', methods=['POST'])
def delete_all():
    requests.post(
        "http://34.238.33.155:80/api/v1/db/write",
        data=json.dumps({
            "ORIGIN": "USER",
            "COMMAND": "DELETE_ALL"
        })
    )
    return make_response('', 200)


# api 12
@app.route('/api/v1/_count', methods=['GET'])
def count_requests():
    msg = requests.get(
        "http://34.238.33.155:80/api/v1/db/read",
        params={
            "ORIGIN": "USER",
            "COMMAND": "READ_REQUEST_COUNT"
        }
    )
    if(msg.status_code == 200):
        return make_response(jsonify(msg.json()['count']), 200)


# api 13
@app.route('/api/v1/_count', methods=['DELETE'])
def reset_request_count():
    requests.post(
        "http://34.238.33.155:80/api/v1/db/write",
        data=json.dumps({
            "ORIGIN": "USER",
            "COMMAND": "RESET_REQUEST_COUNT"
        })
    )
    return make_response('', 200)


@app.route('/api/v1/users/health_check', methods=['GET'])
def health_check():
    return make_response('', 200)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)
