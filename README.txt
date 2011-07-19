
To build:

mvn clean; mvn -DskipTests package dependency:copy-dependencies


To run:

export DYLD_LIBRARY_PATH=/path/to/haildb
java -cp "target/*:target/dependency/*" com.g414.st8.Main


To do some stuff with the graph endpoint:

curl "http://localhost:8080/1.0/g/all"
curl -X POST "http://localhost:8080/1.0/g;a=1;b=0;c=2"
curl -X POST "http://localhost:8080/1.0/g;a=1;b=0;c=3"
curl -X POST "http://localhost:8080/1.0/g;a=1;b=0;c=4"
curl -X POST "http://localhost:8080/1.0/g;a=1;b=0;c=5"
curl -X POST "http://localhost:8080/1.0/g;a=1;b=1;c=7"
curl -X POST "http://localhost:8080/1.0/g;a=1;b=1;c=8"
curl -X POST "http://localhost:8080/1.0/g;a=2;b=0;c=1"
curl -X POST "http://localhost:8080/1.0/g;a=2;b=0;c=7"
curl -X POST "http://localhost:8080/1.0/g;a=2;b=0;c=8"
curl -X POST "http://localhost:8080/1.0/g;a=7;b=0;c=9"

curl "http://localhost:8080/1.0/g/all"

curl "http://localhost:8080/1.0/g/bfs?a=1&b=0&maxDepth=1"
curl "http://localhost:8080/1.0/g/bfs?a=1&b=0&maxDepth=3"

