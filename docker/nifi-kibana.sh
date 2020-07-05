#!/bin/bash
# Run docker for Euronews project
# Author Lorenzo Allori <lorenzo.allori@gmail.com>
# v 1.0 - 05/07/2020

howtoaccess() {
	echo ""
	echo "HOW TO ACCESS:"
	echo ""
	echo "To access Nifi please open your browser on"
	echo "http://localhost:8080/nifi"
	echo ""
	echo "To acess Kibana please open your browser on"
	echo "http://locahost:5601/"
	echo ""
	echo ""
}

fresh() {
	echo "Creating a fresh install of NIFI+ELASTICSEARCH+KIBANA"
	sudo docker container stop nifi
	sudo docker container stop kibana
	sudo docker container rm nifi
	sudo docker container rm kibana

	# with network --- do not use --
	#sudo docker run -d --network cyberint --name kibana --mount type=bind,source=/home/lorenzo/Documents,target=/opt/kibana/data -p 9200:9200 -p 5601:5601 nshou/elasticsearch-kibana
	#sudo docker run --network cyberint --name nifi --mount type=bind,source=/home/lorenzo/Documents/Development/EuronewsXMLCorpus/euronewsproject-xml-corpus/xmloutput,target=/opt/nifi/data/input --mount type=bind,source=/home/lorenzo/Documents/Development/EuronewsXMLCorpus/euronewsproject-xml-corpus/nifi,target=/opt/nifi/data/output -p 8080:8080 -d apache/nifi:latest

	# with host network --- use this ---
	sudo docker run -d --network host --name kibana --mount type=bind,source=/home/lorenzo/Documents,target=/opt/kibana/data -p 9200:9200 -p 5601:5601 nshou/elasticsearch-kibana
	sudo docker run --network host --name nifi --mount type=bind,source=/home/lorenzo/Documents/Development/EuronewsXMLCorpus/euronewsproject-xml-corpus/xmloutput,target=/opt/nifi/data/input --mount type=bind,source=/home/lorenzo/Documents/Development/EuronewsXMLCorpus/euronewsproject-xml-corpus/nifi,target=/opt/nifi/data/output -p 8080:8080 -d apache/nifi:latest
	echo "Loading...."
	sleep 3
	howtoaccess
	status

}


start() {
	echo ""
	echo "Starting Apache Nifi.."	
	sudo docker container start nifi
	echo "Starting Elastic+Kibana.."
	sudo docker container start kibana
	echo "Loading..."
	sleep 3
	howtoaccess
	status
}

stop() {
	echo ""
	echo "Stopping Apache Nifi.."
	sudo docker container stop nifi
	echo "Stopping Elastic+Kibana.."
	sudo docker container stop kibana
	status
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