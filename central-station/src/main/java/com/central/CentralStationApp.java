package com.central;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
// import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
// Add these imports
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.central.Bitcask.BitcaskServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;


public class CentralStationApp {

    private static final String TOPIC = "weather-data"; //MESSAGE CHANNEL PATTERN

    public static void main(String[] args) throws Exception {

        String bitcaskDir = System.getenv("BITCASK_DATA_DIR");
        if (bitcaskDir == null) {
            bitcaskDir = "./bitcask-data"; 
        }

        KafkaConsumer<String, String> consumer = null;
        KafkaProducer<String, String> invalidMessageProducer = null;
        Parquet parquet = null;

        System.out.println("LAUNCHING CENTRAL BASE STATION <3");
        try {
            // 1. Open Bitcask
            BitcaskServer bitcask = BitcaskServer.open(bitcaskDir);
            System.out.println("init bitcask storage engine");

            // 2. Initialize Parquet Writer / Buffer
            parquet = new Parquet();
            System.out.println("init parquet done");

            // 3. HTTP Server
            HttpServer httpServer = HttpServer.create(new InetSocketAddress(8080), 0); //default backlog queue size

            // ENDPTS
            // GET /bitcask/all (returns all latest station states)
            httpServer.createContext("/bitcask/all", (HttpExchange exchange) -> {
                try {
                    Map<String, String> all = bitcask.getAll();
                    StringBuilder json = new StringBuilder("{");
                    boolean first = true;
                    for (Map.Entry<String, String> entry : all.entrySet()) {
                        if (!first) json.append(",");
                        json.append("\"").append(entry.getKey()).append("\":").append(entry.getValue());
                        first = false;
                    }
                    json.append("}");
                    byte[] response = json.toString().getBytes(); //http sends bytes not strings
                    exchange.sendResponseHeaders(200, response.length); //200: OK
                    try (OutputStream os = exchange.getResponseBody()) { //auto closes stream safely
                        os.write(response);
                    }
                } catch (Exception e) {
                    byte[] err = e.getMessage().getBytes();
                    exchange.sendResponseHeaders(500, err.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(err);
                    }
                }
            });

            // GET /bitcask/key/{key} (returns value of a specific station)
            httpServer.createContext("/bitcask/key/", (HttpExchange exchange) -> {
                try {
                    String path = exchange.getRequestURI().getPath(); 
                    String key = path.substring("/bitcask/key/".length());
                    String value = bitcask.get(key);
                    byte[] response;
                    if (value == null) {
                        response = "NOT FOUND".getBytes();
                        exchange.sendResponseHeaders(404, response.length);
                    } else {
                        response = value.getBytes();
                        exchange.sendResponseHeaders(200, response.length);
                    }
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response);
                    }
                } catch (Exception e) {
                    byte[] err = e.getMessage().getBytes();
                    exchange.sendResponseHeaders(500, err.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(err);
                    }
                }
            });

            httpServer.start();
            System.out.println("[HTTP] Server started on port 8080");


            // 3.  Kafka Consumer
            Properties props = new Properties();

            String bootstrapServers = System.getenv().getOrDefault("KAFKA_SERVERS", "localhost:9092");
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group-" + System.currentTimeMillis()); //Q: 
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //Q: MAKE IT LATEST?
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //CHANGED KHALETHA LATEST

            // props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group");
            // props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); //Q:
            // props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(TOPIC));
            
            System.out.println("[KAFKA] Connected to broker. Listening on topic: " + TOPIC);

            // Invalid Message Channel - dead letter producer
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            invalidMessageProducer = new KafkaProducer<>(producerProps);
            System.out.println("[INVALID-MESSAGE CHANNEL] Dead letter producer initialized");
            
            // System.out.println("Central Station running smoothly");

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //pull-based polling //POLLING CONSUMER PATTERN

                // System.out.println("manouna is debugging, poll completed. found records count: " + records.count());
                
                
                for (ConsumerRecord<String, String> record : records) {
                    String rawJson = record.value();
                    String stationKey = record.key(); // station_id passed as the partition key

                    if (stationKey == null) {
                        stationKey = "unknown_station";
                    }

                    // INVALID MESSAGE PATTERN (Dead Letter)  
                    if (rawJson == null || !rawJson.trim().startsWith("{")) {
                        System.err.println("[INVALID-MESSAGE CHANNEL] Routing poison pill to dead-letter topic: " + rawJson);
                        invalidMessageProducer.send(new ProducerRecord<>("invalid-messages", stationKey, rawJson));
                        continue; 
                    }

                    System.out.println("[INGEST] Received stream packet from Station " + stationKey);

                    // Operation A: Update the Bitcask Key-Value view instantly
                    bitcask.put(stationKey, rawJson);
                    System.out.println("[BITCASK] Stored key: " + stationKey);
                    // String readBack = bitcask.get(stationKey);
                    // System.out.println("[BITCASK] Read back for key " + stationKey + ": " + readBack);
                    try {
                        parquet.write(rawJson);
                        // Operation C: Send to ElasticSearch
                        String esHost = System.getenv().getOrDefault("ELASTICSEARCH_HOST", "localhost:9200");
                        URI uri = new URI("http://" + esHost + "/weather-data/_doc");
                        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
                        conn.setRequestMethod("POST");
                        conn.setRequestProperty("Content-Type", "application/json");
                        conn.setDoOutput(true);
                        conn.getOutputStream().write(rawJson.getBytes());
                        conn.getResponseCode();
                        conn.disconnect();
                    } catch (Exception e) {
                        System.err.println("[PARQUET ERROR] " + e.getMessage());
                        e.printStackTrace();
                    }

                }
            }


        } catch (Exception e) {
            System.err.println("Error running ingestion worker: " + e.getMessage());
            e.printStackTrace();
        } finally {

            if (consumer != null) {
                System.out.println("Closing Kafka consumer network ports...");
                consumer.close();
            }
            if (parquet != null) {
                parquet.close();
            }
            if (invalidMessageProducer != null) invalidMessageProducer.close();
        }
    }
}