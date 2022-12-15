docker build -t test .
docker run --rm -e ENABLE_INIT_DAEMON=false --network big-data-network --name python-client test 
