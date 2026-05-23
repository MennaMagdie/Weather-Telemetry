mvn exec:java -Dexec.mainClass="com.central.CentralStationApp"



docker exec -it local-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic weather-data --property "parse.key=true" --property "key.separator=:"


![alt text](<Screenshot from 2026-05-18 13-06-36.png>)



----


kubectl port-forward -n pipeline service/central-station-service 8080:8080


export CENTRAL_STATION_URL="http://localhost:8080"
cd /media/mennamagdy/Data/Uni/Term-10/DDIA/Weather-Telemetry
./bitcask_client.sh --view --key=1


# View all keys
./bitcask_client.sh --view-all

# Check the generated CSV file
ls *.csv
cat <timestamp>.csv