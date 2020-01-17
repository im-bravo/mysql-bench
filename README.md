# 
```sh
go get github.com/im-bravo/mysql-bench
cd ~/go/bin
```

```sh
./mysql-bench -workload uniform -mode write -concurrency 100 -keyspace dbname -duration 5s -nodes 192.168.58.100:3306 -table grant_standard -username user001 -password 'pwd'
```


```sh
go run main.go modes.go workloads.go -workload uniform -mode write_deep -concurrency 1 -keyspace onepoint -duration 5s -nodes 192.168.58.100:3306 -table grant_standard -username root -password 'example'

```