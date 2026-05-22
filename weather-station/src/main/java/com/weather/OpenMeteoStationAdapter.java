package com.weather;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
     // "current": {
        //     "time": "2026-05-22T12:30",
        //     "interval": 900,
        //     "temperature_2m": 23.6,
        //     "relative_humidity_2m": 65,
        //     "wind_speed_10m": 16.2
        // }
        // }
public class OpenMeteoStationAdapter {
    private static final String CONNECTION_API_URL = "https://api.open-meteo.com/v1/forecast?latitude=31.2018&longitude=29.9158&current=temperature_2m,relative_humidity_2m,wind_speed_10m";
    private ObjectMapper objectMapper;
    private long sequenceNumber=1; 
    private long STATION_ID = 11;
    private HttpClient httpclient;
    private WeatherStationProducer producer;

    public OpenMeteoStationAdapter() {
        // this.httpclient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
        this.httpclient = HttpClient.newHttpClient();
        this.producer = new WeatherStationProducer();

    }

    private String getOpenMeteoData() throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(CONNECTION_API_URL)).GET().build();
        HttpResponse<String> apiReply = httpclient.send(req, HttpResponse.BodyHandlers.ofString());
        return apiReply.body();
    }

    private WeatherStatusMessage adapWeatherStatusMessage(double temp, int humidity, int windspeed){
        WeatherStatusMessage msg = new WeatherStatusMessage(
            STATION_ID,
            sequenceNumber++,
            "high",
            System.currentTimeMillis()/1000L,
            new WeatherDetails(humidity, (int)temp, windspeed)
        );
        return msg;
    }

    public void sendOpenMeteoData() {
        while(true){try {
            String apiResponse = getOpenMeteoData();
            // json parsing and formatting to weatherStatus msg type:
            JsonNode rootNode = objectMapper.readTree(apiResponse);
            JsonNode currentWeather = rootNode.path("current");
            double temp = currentWeather.path("temperature_2m").asDouble();
            int humidity = currentWeather.path("relative_humidity_2m").asInt();
            int windspeed = currentWeather.path("wind_speed_10m").asInt();

            WeatherStatusMessage msg = adapWeatherStatusMessage(temp, humidity, windspeed);
            // String jsonMsg = objectMapper.writeValueAsString(msg);
            producer.sendMessages(msg); // send to Kafka

            System.out.println("Fetched OpenMeteo data and sent to Kafka: " + objectMapper.writeValueAsString(msg));

            Thread.sleep(10000); //10 secs bw every fetch 

        } catch (IOException | InterruptedException e) {
            System.err.println("Error fetching or processing OpenMeteo data: " + e.getMessage());
        }}
        
    }

    public static void main(String[] args) {
        OpenMeteoStationAdapter adapter = new OpenMeteoStationAdapter();
        adapter.sendOpenMeteoData();
    }

}
