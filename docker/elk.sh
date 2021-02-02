#!/bin/bash
# This script can be used to run both nifi and elastisearch+kibana docker images.
# Author Lorenzo Allori <lorenzo.allori@gmail.com>
# v 1.1 - 19/07/2020
#
# Run options are:
# {start|stop|restart|status|fresh}
# the "fresh" is used to run them for the first time (or to have a clean version of the docker images)
#
# HINT: pls edit the variables for your local data paths 

# Versions
ELKVER="7.8.0"

# Please edit with you own dirs (sharing between host system and docker containers
NIFIDIRINPUT="/home/lorenzo/Documents/Development/EuronewsXMLCorpus/euronews-xml-corpus/nifi/input"
NIFIDIROUTPUT="/home/lorenzo/Documents/Development/EuronewsXMLCorpus/euronews-xml-corpus/nifi/output"
KIBANADIROUTPUT="/home/lorenzo/Documents/Development/EuronewsXMLCorpus/euronews-xml-corpus/kibana"


# Please do not edit here under unless you do not know what you are doing.

howtoaccess() {
	echo ""
	echo "HOW TO ACCESS:"
	echo ""
	#echo "To access Nifi please open your browser on"
	#echo "http://localhost:8080/nifi"
	echo ""
	echo "To acess Kibana please open your browser on"
	echo "http://locahost:5601/"
	echo ""
	echo ""
}

fresh() {
	echo "Creating a fresh install of NIFI+ELASTICSEARCH+KIBANA"
	#sudo docker container stop nifi
	sudo docker container stop kibana
	sudo docker container stop elasticsearch
	#sudo docker container rm nifi
	sudo docker container rm kibana
	sudo docker container rm elasticsearch

	# with network --- do not use --
	#sudo docker run -d --network cyberint --name kibana --mount type=bind,source=/home/lorenzo/Documents,target=/opt/kibana/data -p 9200:9200 -p 5601:5601 nshou/elasticsearch-kibana
	#sudo docker run --network cyberint --name nifi --mount type=bind,source=/home/lorenzo/Documents/Development/EuronewsXMLCorpus/euronewsproject-xml-corpus/xmloutput,target=/opt/nifi/data/input --mount type=bind,source=/home/lorenzo/Documents/Development/EuronewsXMLCorpus/euronewsproject-xml-corpus/nifi,target=/opt/nifi/data/output -p 8080:8080 -d apache/nifi:latest

	# with host network --- use this ---
	#sudo docker run -d --network host --name nifi --mount type=bind,source=${NIFIDIRINPUT},target=/opt/nifi/data/input --mount type=bind,source=${NIFIDIROUTPUT},target=/opt/nifi/data/output -p 8080:8080 -d apache/nifi:latest
	sudo docker run -d --network host --name kibana --mount type=bind,source=${KIBANADIROUTPUT},target=/opt/kibana/data -p 5601:5601 kibana:${ELKVER}
	sudo docker run -d --network host --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:${ELKVER}	
	echo "Loading...."
	sleep 3
	status

}


start() {
	echo ""
	#echo "Starting Apache Nifi.."	
	#sudo docker container start nifi
	echo "Starting Elastic+Kibana.."
	sudo docker container start kibana
	sudo docker container start elasticsearch
	echo "Loading..."
	sleep 3
	howtoaccess
	status
}

stop() {
	echo ""
	#echo "Stopping Apache Nifi.."
	#sudo docker container stop nifi
	echo "Stopping Elastic+Kibana.."
	sudo docker container stop kibana
	sudo docker container stop elasticsearch
	status
}

restart() {
	stop
	start
} 


status() {
	echo ""
	echo "--- DOCKER STATUS ---"
	echo ""
	sudo docker container ls

}

case "$1" in
        start)
            start
            ;;
        stop)
            stop
            ;;
	restart)
	    stop
	    start
	    ;; 
        status)
            status
            ;;
        fresh)
            fresh
            ;; 
        *)
            echo $"Usage: $0 {start|stop|restart|status|fresh}"
            echo "fresh option is to create a fresh install of the containers"
            exit 1
esac