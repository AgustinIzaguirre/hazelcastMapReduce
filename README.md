# hazelcastMapReduce
# POD TP 2 - Hazelcast

Distributed Processing with MapReduce for airports information.

## Prerequisites
1. Running on Linux or OSX.
2. Having maven installed.
3. Having Java 8 or later versions.

## Building
1. cd to folder containing .tar.gz
1. `tar -xvf POD_TPE2_G12.tar.gz`
1. `cd hazelcastMapReduce`
1. `mvn clean install`

## Running from source code
After executing `mvn clean install` on the same directory:

### Running server
Run with CWD in root folder
1. `cd server/target`
1. `tar -xvf hazelcastMapReduce-server-1.0-SNAPSHOT-bin.tar.gz`
1. `cd hazelcastMapReduce-server-1.0-SNAPSHOT`
1. `chmod u+x *.sh`
1. `bash ./run-server`

### Running client
#### Query 1
Run with CWD in root folder

1. `cd client/target/`
1. `tar -xvf hazelcastMapReduce-client-1.0-SNAPSHOT-bin.tar.gz`
1. `cd hazelcastMapReduce-client-1.0-SNAPSHOT`
1. `chmod u+x query*`
1. `bash ./query1 -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX
         -DoutPath=YY
         
 TODO specify parameters
 
#### Query 2
Run with CWD in root folder
 
1. `cd client/target/`
1. `tar -xvf hazelcastMapReduce-client-1.0-SNAPSHOT-bin.tar.gz`
1. `cd hazelcastMapReduce-client-1.0-SNAPSHOT`
1. `chmod u+x query*`
1. `bash ./query2 -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX
          -DoutPath=YY -Dn=N
          
 TODO specify parameters
 
#### Query 3
Run with CWD in root folder

1. `cd client/target/`
1. `tar -xvf hazelcastMapReduce-client-1.0-SNAPSHOT-bin.tar.gz`
1. `cd hazelcastMapReduce-client-1.0-SNAPSHOT`
1. `chmod u+x query*`
1. `bash ./query3 -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX
       -DoutPath=YY
       
TODO specify parameters


#### Query 4
Run with CWD in root folder

1. `cd client/target/`
1. `tar -xvf hazelcastMapReduce-client-1.0-SNAPSHOT-bin.tar.gz`
1. `cd hazelcastMapReduce-client-1.0-SNAPSHOT`
1. `chmod u+x query*`
1. `bash ./query4 -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX
       -DoutPath=YY -Doaci=CODE -Dn=N
       
TODO specify parameters