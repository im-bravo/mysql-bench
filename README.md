# 
go get github.com/im-bravo/mysql-bench
cd ~/go/bin

./mysql-bench -workload uniform -mode write -concurrency 100 -keyspace dbname -duration 5s -nodes 100.18.0.4:3306 -table grant_standard -username onepoint_user -password 'pwd'

