# dkv
Distributed Key Value Store


# Run server
go run cmd/main.go --http-port 8081 --raft-port 2021 --id 1
go run cmd/main.go --http-port 8082 --raft-port 2022 --id 2 
go run cmd/main.go --http-port 8083 --raft-port 2023 --id 3

# Join other nodes
curl 'localhost:8081/join?addr=localhost:2022&id=2'
curl 'localhost:8081/join?addr=localhost:2023&followerId=3'

# Query the KV Store

## Set 
curl 'localhost:8081/set?key=x&value=2'

## Get
curl 'localhost:8081/get?key=x'