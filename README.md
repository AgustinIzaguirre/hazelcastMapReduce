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
1. `mvn clean install -DskipTests=true`

## Running from source code
After executing `mvn clean install` on the same directory:

### Running server
Run with CWD in root folder
1. `cd server/target`
1. `tar -xvf hazelcastMapReduce-server-1.0-SNAPSHOT-bin.tar.gz`
1. `cd hazelcastMapReduce-server-1.0-SNAPSHOT`
1. `chmod u+x *.sh`
1. `check that hazelcast.xml configuraion of network > interfaces > interface 
pattern matches your network by default is set to ips of type 10.xx.xx.xx `
1. `bash ./run-server.sh`

### Running client
#### Query 1
Run with CWD in root folder

1. `cd client/target/`
1. `tar -xvf hazelcastMapReduce-client-1.0-SNAPSHOT-bin.tar.gz`
1. `cd hazelcastMapReduce-client-1.0-SNAPSHOT`
1. `chmod u+x query*`
1. `bash ./query1.sh -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX
        -DoutPath=YY`

 Where addresses is a list of ip (xx.xx.xx.xx) and port (XXXX).
 inPath is a path to the folder containing "aeropuetos.csv" and "movimientos.csv".
 outPath is a path to the folder where the file "query1.csv" and "query1.txt" are going to be created.

#### Query 2
Run with CWD in root folder

1. `cd client/target/`
1. `tar -xvf hazelcastMapReduce-client-1.0-SNAPSHOT-bin.tar.gz`
1. `cd hazelcastMapReduce-client-1.0-SNAPSHOT`
1. `chmod u+x query*`
1. `bash ./query2.sh -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX
          -DoutPath=YY -Dn=N`

Where addresses is a list of ip (xx.xx.xx.xx) and port (XXXX).
inPath is a path to the folder containing "aeropuetos.csv" and "movimientos.csv".
outPath is a path to the folder where the file "query2.csv" and "query2.txt" are going to be created.
n is the quantity of airlines

#### Query 3
Run with CWD in root folder

1. `cd client/target/`
1. `tar -xvf hazelcastMapReduce-client-1.0-SNAPSHOT-bin.tar.gz`
1. `cd hazelcastMapReduce-client-1.0-SNAPSHOT`
1. `chmod u+x query*`
1. `bash ./query3.sh -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX
       -DoutPath=YY`

Where addresses is a list of ip (xx.xx.xx.xx) and port (XXXX).
inPath is a path to the folder containing "aeropuetos.csv" and "movimientos.csv".
outPath is a path to the folder where the file "query3.csv" and "query3.txt" are going to be created.


#### Query 4
Run with CWD in root folder

1. `cd client/target/`
1. `tar -xvf hazelcastMapReduce-client-1.0-SNAPSHOT-bin.tar.gz`
1. `cd hazelcastMapReduce-client-1.0-SNAPSHOT`
1. `chmod u+x query*`
1. `bash ./query4.sh -Daddresses='xx.xx.xx.xx:XXXX;yy.yy.yy.yy:YYYY' -DinPath=XX
       -DoutPath=YY -Doaci=CODE -Dn=N`

Where addresses is a list of ip (xx.xx.xx.xx) and port (XXXX).
inPath is a path to the folder containing "aeropuetos.csv" and "movimientos.csv".
outPath is a path to the folder where the file "query4.csv" and "query4.txt" are going to be created.
oaci is the oaci code of the airport that the movement starts in.
n is the number of airporst in the answer.
