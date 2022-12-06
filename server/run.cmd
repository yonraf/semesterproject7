docker build -t node-server .
docker run --rm --network big-data-network -p 3000:3000 node-server