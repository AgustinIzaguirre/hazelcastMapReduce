#!/bin/bash
if [ $# -ne 5 ]
then
	echo -e "Ussage:\n bash ./query1 -Daddresses='serverAddresses' -DinPath=inputDirectory -DoutPath=outputDirectory -Doaci=originOaci -Dn=NumberOfAirports";
else
    java -DqueryNumbber=4 $1 $2 $3 $4 $5 -cp 'lib/jars/*' "ar.edu.itba.pod.client.Client"
fi
