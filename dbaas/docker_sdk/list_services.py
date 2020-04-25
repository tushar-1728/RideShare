import docker

client = docker.from_env()
a = client.containers.list()
print(a)