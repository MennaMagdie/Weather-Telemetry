// src/test/java/com/example/Bitcask/BitcaskServerTest.java
package com.central.Bitcask;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;

class BitcaskServerTest {

    // runs before each test — fresh directory
    /*
    @BeforeEach
    void setup() throws IOException {
        cleanup("./test-data");
    }
    */
    @Test
    void testHintFileCreatedAfterMerge() throws Exception {
        BitcaskServer db = BitcaskServer.open("./test-data");
        
        for (int i = 0; i < 1000; i++) {
            db.put("key" + i, "value" + i);
        }
        
        db.merge();
        db.close();
        
        // print all files in directory to see what's there
        System.out.println("Files after merge:");
        Files.list(Paths.get("./test-data"))
            .forEach(p -> System.out.println("  " + p.getFileName()));
        
        long hintCount = Files.list(Paths.get("./test-data"))
            .filter(p -> p.toString().endsWith(".hint"))
            .count();
        
        System.out.println("Hint count: " + hintCount);
        assertTrue(hintCount > 0);
    }
    /*
    // runs after each test — clean up files
    @AfterEach
    void teardown() throws IOException {
        cleanup("./test-data");
    }

    // ── Tests ────────────────────────────────────────

    @Test
    void testBasicPutGet() throws Exception {
        BitcaskServer db = BitcaskServer.open("./test-data");

        db.put("name", "omnia");
        assertEquals("omnia", db.get("name"));

        db.put("name", "updated");
        assertEquals("updated", db.get("name"));

        db.close();
    }

    @Test
    void testRestartRecovery() throws Exception {
        BitcaskServer db = BitcaskServer.open("./test-data");
        db.put("key1", "value1");
        db.put("key1", "updated");
        db.close();

        // reopen — simulates restart
        db = BitcaskServer.open("./test-data");
        assertEquals("updated", db.get("key1"));
        assertNull(db.get("nonexistent"));
        db.close();
    }

    @Test
    void testMergeKeepsLatestValue() throws Exception {
        BitcaskServer db = BitcaskServer.open("./test-data");

        for (int i = 0; i < 100; i++) {
            db.put("key", "value_" + i);
        }
        db.put("user", "omnia");

        db.merge();

        assertEquals("value_99", db.get("key"));
        assertEquals("omnia", db.get("user"));

        db.close();

        // restart after merge
        db = BitcaskServer.open("./test-data");
        assertEquals("value_99", db.get("key"));
        assertEquals("omnia", db.get("user"));
        db.close();
    }

    @Test
    void testNullOnMissingKey() throws Exception {
        BitcaskServer db = BitcaskServer.open("./test-data");
        assertNull(db.get("doesnotexist"));
        db.close();
    }
    @Test
    void testLatestKafkaMessageAlwaysReturned() throws Exception {
        BitcaskServer db = BitcaskServer.open("./test-data");
        
        // simulate kafka messages arriving for station "station_1"
        String[] messages = {
            "{\"temp\": 20, \"seq\": 1}",
            "{\"temp\": 22, \"seq\": 2}",
            "{\"temp\": 19, \"seq\": 3}"  // latest
        };
        
        for (String msg : messages) {
            db.put("station_1", msg);
        }
        
        String latest = db.get("station_1");
        assertTrue(latest.contains("\"seq\": 3"));  // must be latest message
        
        db.close();
    }
    @Test
    void testConcurrentAccess() throws Exception {
        BitcaskServer db = BitcaskServer.open("./test-data");
        
        int threadCount = 10;
        int writesPerThread = 1000;
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Exception> errors = Collections.synchronizedList(new ArrayList<>());
        
        // concurrent writers
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    for (int i = 0; i < writesPerThread; i++) {
                        db.put("key_" + threadId, "value_" + i);
                    }
                } catch (Exception e) {
                    errors.add(e);
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        
        latch.await();
        
        assertTrue(errors.isEmpty(), "Concurrent writes caused errors: " + errors);
        
        // verify all keys exist
        for (int t = 0; t < threadCount; t++) {
            assertNotNull(db.get("key_" + t));
        }
        
        db.close();
    }
    // ── Helper ───────────────────────────────────────

    private void cleanup(String dirPath) throws IOException {
        File dir = new File(dirPath);
        if (dir.exists()) {
            // delete all files inside first
            for (File f : dir.listFiles()) {
                f.delete();
            }
            dir.delete();
        }
    }
    */
}