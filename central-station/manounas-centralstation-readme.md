mvn exec:java -Dexec.mainClass="com.central.CentralStationApp"



docker exec -it local-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic weather-data --property "parse.key=true" --property "key.separator=:"


![alt text](<Screenshot from 2026-05-18 13-06-36.png>)
