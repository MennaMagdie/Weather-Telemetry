package com.weather;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WeatherStationProducer {
    // DONT FORGET TO INCLUDE BONUS HERE AS WELL (SAME WRAPPER CLASS)

    private static final String TOPIC = "weather-data";
    // private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    // private static final long STATION_ID = 1; 
    private static final long STATION_ID = Long.parseLong(System.getenv().getOrDefault("STATION_ID", "1"));
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_SERVERS", "localhost:9092");
    
    private final KafkaProducer<String, String> producer;
   //  private final WeatherStationMock weatherStationMock;
    private final ObjectMapper objectMapper;

    public WeatherStationProducer(){
        System.out.println("Connecting to Kafka at: " + BOOTSTRAP_SERVERS); 
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        properties.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "5000");

        this.producer = new KafkaProducer<>(properties);
        // this.weatherStationMock = new WeatherStationMock(STATION_ID); // a weather station for each ID
        this.objectMapper = new ObjectMapper();
    }

    public void sendMessages(WeatherStatusMessage weatherMsg){
        try{
            String jsonWeatherMsg = objectMapper.writeValueAsString(weatherMsg);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, Long.toString(weatherMsg.getStation_id()), jsonWeatherMsg);
            producer.send(record, (metadata, ex) -> {
                if(ex!=null){
                    System.err.println("Failed to send message: " + ex.getMessage());
                } else{
                    System.out.println("Message from station " + weatherMsg.getStation_id() + " sent to topic " + metadata.topic() + " partition " + metadata.partition() + " offset " + metadata.offset());
                }
            });

        } catch (Exception e) {
            System.err.println("Error producing message: " + e.getMessage());
        }
    }
    public void produceMessages(){
        // unique per object??
        while(true){
            try {
                WeatherStatusMessage weatherMsg = new WeatherStationMock(STATION_ID).sendNewMessage();
                if(weatherMsg != null){
                    sendMessages(weatherMsg);
                }
                Thread.sleep(1000); // 1 sec bw msgs
            } catch (InterruptedException e) {
                System.err.println("Producer interrupted: " + e.getMessage());
                break;
            }
        }
    }
    public static void main(String[] args){
        WeatherStationProducer producer = new WeatherStationProducer();
        producer.produceMessages();
    }
}