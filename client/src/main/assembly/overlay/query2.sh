#!/bin/bash
if [ $# -ne 4 ]
then
	echo -e "Ussage:\n bash ./query1 -Daddresses='serverAddresses' -DinPath=inputDirectory -DoutPath=outputDirectory -Dn=NumberOfAirlines";
else
    java -DqueryNumbber=2 $1 $2 $3 $4 -cp 'lib/jars/*' "ar.edu.itba.pod.client.Client"
fi
