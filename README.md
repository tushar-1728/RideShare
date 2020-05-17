# RideShare
CREATING USERS AND RIDES CONTAINER

1: Goto folder user-ride

2: Create image python:pip using Dockerfile in that folder
3: Goto folder user inside user-ride folder, run users container using docker-compose file
4: Goto folder ride inside user-ride folder, run rides container using docker-compose file

CREATING DBASS CONTAINER
0: Goto dbaas folder
1: Goto orch-docker folder, create python:project image using Dockerfile in that folder
2: Goto work-docker folder, create python:worker image using Dockerfile in that folder
3: Now goto project folder, create worker:latest image using Dockerfile_worker in that folder
4: Run the dbaas container using docker-compose file in project folder
