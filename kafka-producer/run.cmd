docker build -t test .
docker run --rm --network big-data-network --name python-client test