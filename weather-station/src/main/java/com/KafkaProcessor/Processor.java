package com.KafkaProcessor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

public class Processor {
    public static void main(String[] args) {
        // Config
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app-2"); // unique name for the stream app
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // connects the app to Kafka server
        
        // kafka msgs are treated as string (key = device, value = "weather: {humidity:...}")
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ObjectMapper mapper = new ObjectMapper(); // to convert JSON string to structured object

        // Topology
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("weather-data"); // read from input topic
        source.filter((k, v) -> {

            try {
                /*
                    input:

                    {
                        "weather": {
                            "humidity": 80,
                            "temperature": 100
                        }
                    }

                    output becomes a tree:

                    root
                    └── weather
                        └── humidity = 80
                */

                JsonNode root = mapper.readTree(v);

                int humidity =
                    root.get("weather")
                        .get("humidity")
                        .asInt();

                return humidity > 70;

            } catch (Exception e) {

                return false;
            }

        }).to("rain-alerts");

        // Start
        KafkaStreams streams = new KafkaStreams(builder.build(), config); // compiles your topology into a running engine
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}   
