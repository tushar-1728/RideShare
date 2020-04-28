docker run -itd --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.8.3-alpine
docker exec -it rabbitmq /bin/sh -c "rabbitmq-plugins enable rabbitmq_management"