docker build . -t pysparkexampleimage:latest 
docker run --rm -e ENABLE_INIT_DAEMON=false --network big-data-network -p 3000:3000 --name pyspark3 pysparkexampleimage