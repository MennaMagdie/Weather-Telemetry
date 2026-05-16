package com.example;
import com.example.Bitcask.BitcaskServer;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {

        BitcaskServer bitcask = BitcaskServer.open("./test-data");

        // Simulate weather messages as plain strings
        bitcask.put("station_1", "{\"humidity\":80, \"temperature\":25}");
        bitcask.put("station_2", "{\"humidity\":60, \"temperature\":30}");
        bitcask.put("station_3", "{\"humidity\":45, \"temperature\":22}");

        // Test get single
        System.out.println(bitcask.get("station_1"));  // should print station_1 json

        // Test update — put a newer value for station_1
        bitcask.put("station_1", "{\"humidity\":90, \"temperature\":27}");
        System.out.println(bitcask.get("station_1"));  // should print the NEWER json

        // Test getAll
        Map<String, String> all = bitcask.getAll();
        all.forEach((k, v) -> System.out.println(k + " → " + v));

        // Test segment rotation — temporarily lower the limit
        // then spam enough puts to force a new segment file to appear
        for (int i = 4; i <= 20; i++) {
            bitcask.put("station_" + i, "{\"humidity\":" + i + "}");
        }

        // After spamming, getAll should still return everything correctly
        System.out.println("Total keys: " + bitcask.getAll().size());

        bitcask.close();
    }
}