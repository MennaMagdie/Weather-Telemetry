package com.central;
import java.util.Map;

import com.central.Bitcask.BitcaskServer;

public class testmain {
    public static void main(String[] args) throws Exception {
        
        BitcaskServer db = BitcaskServer.open("./test-data"); 

        for (int i = 0; i < 100; i++) {
            db.put("key", "value_" + i);
        }

        db.put("user", "omnia");
        Thread.sleep(15);

        db.merge();

        System.out.println("----- BEFORE CLOSING -----");
        System.out.println(db.get("key"));
        System.out.println(db.get("user"));

        db.close();
        

        // RESTART
        System.out.println("Restarting ....");
        db = BitcaskServer.open("./test-data");

        System.out.println(db.get("key"));
        System.out.println(db.get("user"));

        db.close();

        System.out.println("-------------- Finito -------------");
    }
    
    public static void main2(String[] args) throws Exception {

        BitcaskServer bitcask = BitcaskServer.open("./test-data");

        // Simulate weather messages as plain strings
        bitcask.put("station_1", "{\"humidity\":80, \"temperature\":25}");
        bitcask.put("station_2", "{\"humidity\":60, \"temperature\":30}");
        bitcask.put("station_3", "{\"humidity\":45, \"temperature\":22}");

        // Test get single
        System.out.println("Station 1 :" + bitcask.get("station_1"));  // should print station_1 json

        // Test update — put a newer value for station_1
        bitcask.put("station_1", "{\"humidity\":90, \"temperature\":27}");
        System.out.println("Station 1 after update : " + bitcask.get("station_1"));  // should print the NEWER json

        // Test getAll
        Map<String, String> all = bitcask.getAll();
        all.forEach((k, v) -> System.out.println(k + " : " + v));

        // Test segment rotation — temporarily lower the limit
        // then spam enough puts to force a new segment file to appear
        for (int i = 4; i <= 20; i++) {
            boolean ok = bitcask.put("station_" + i, "{\"humidity\":" + i + "}");
            //System.out.println("put success ?" + ok );
        }

        // After spamming, getAll should still return everything correctly
        all = bitcask.getAll();
        System.out.println("Total keys: " + all.size());
        // all.forEach((k, v) -> System.out.println(k + " : " + v));

        bitcask.close();
    }
}