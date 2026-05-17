package com.weather;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Random;
import java.time.Instant;

public class WeatherStationMock {
    private final long stationId;
    private long sequenceNumber = 1; 
    private final Random random = new Random();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WeatherStationMock(long stationId) {
        this.stationId = stationId;
    }

    protected boolean dropMessage() {
        return random.nextDouble() < 0.10; 
    }

    private String generateBatteryStatus() {
        double chance = random.nextDouble();
        if (chance < 0.30) {
            return "low";      
        } else if (chance < 0.70) {
            return "medium";    
        } else {
            return "high";      
        }
    }

    public WeatherStatusMessage generateMessage() {
        int humidity = random.nextInt(101); 
        int temperature = 32 + random.nextInt(90);
        int windSpeed = random.nextInt(251); 

        WeatherDetails details = new WeatherDetails(humidity, temperature, windSpeed);
        String battery = generateBatteryStatus();
        long currentUnixTimestamp = Instant.now().getEpochSecond();

        return new WeatherStatusMessage(
                this.stationId,
                this.sequenceNumber++,
                battery,
                currentUnixTimestamp,
                details
        );
    }

    public WeatherStatusMessage sendNewMessage(){
        if (dropMessage()) {
            System.out.println("[SIMULATION] Message dropped dynamically (10% rate).");
            return null; 
        }
        return generateMessage();
    }
    // public void startSimulation() {
        // System.out.println("Starting Weather Station Mock for ID: " + this.stationId);
        
        // while (true) {
        //     try {
        //         Thread.sleep(1000);

        //         if (dropMessage()) {
        //             System.out.println("[SIMULATION] Message dropped dynamically (10% rate).");
        //             continue; 
        //         }

        //         WeatherStatusMessage message = generateMessage();

        //         // using object mapper instead of manualy doing it 
        //         // String json = "{\n" +
        //         //             "  \"station_id\": " + this.stationId + ",\n" +
        //         //             "  \"s_no\": " + this.sequenceNumber + "\n" +
        //         //             "}";

        //         String jsonMessage = objectMapper.writeValueAsString(message);
                
        //         System.out.println(jsonMessage);

        //     } catch (Exception e) {
        //         System.err.println("Error running simulation: " + e.getMessage());
        //     }
        // }
    // }

    // public static void main(String[] args) {
    //     WeatherStationMock mockStation = new WeatherStationMock(1);
    //     mockStation.startSimulation();
    // }
}