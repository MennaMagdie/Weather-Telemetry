package com.example.Bitcask;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
Design Decisions are 
1) the schedular compaction each how much time ?
2) the max segment size then remarked full = ?
3) hint files save what data for optimized saving and retreiving
*/


public class BitcaskServer{
    public final long MAX_BATCH_SIZE = 10000;  // 10k record
    public final long MAX_SEGMENT_SIZE = 64 * 1024 * 1024;  // 64 MB     // Design Decision
    String directoryPath;

    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    HashMap<String,Segment> segmentMap;
    Segment activeSegment;
    KeyDir keyDir;      // index Map

    private BitcaskServer(){}
    
    public static BitcaskServer open(String directoryPath) throws IOException{
        BitcaskServer server = new BitcaskServer();
        server.directoryPath = directoryPath;

        new File(server.directoryPath).mkdirs();  // create new directory if there isn't one

        server.segmentMap = new HashMap<>();        // could be changed to ordered datastructure?
        server.keyDir = new KeyDir();

        Path dir = Paths.get(server.directoryPath);

        List<Path> dataFiles = Files.list(dir)
                .filter(p -> p.toString().endsWith(".data"))
                .sorted(Comparator.comparingInt(p ->
                        Integer.parseInt(
                                p.getFileName().toString()
                                        .replace("Seg_", "")
                                        .replace(".data", "")
                        )
                ))
                .toList();
        
        for (Path f : dataFiles) {
            String  fileId  = f.getFileName().toString().replace(".data", "");
            Segment seg     = new Segment(server.directoryPath, fileId, false);
            server.segmentMap.put(fileId, seg);
        }
        
        // TODO: fill indexMap from hintfiles saved

        // Fallback: for any segment missing a hint file, scan the original data file
        for (Segment seg : server.segmentMap.values()) {
            File hintFile = new File(server.directoryPath, seg.getFileId() + ".hint");
            if (!hintFile.exists()) {
                HintFile.rebuildFromDataFile(seg, server.keyDir);
            }
        }



        String newId = "Seg_" + (server.segmentMap.size() + 1);
        
        server.activeSegment = new Segment(server.directoryPath,newId,true);
        server.segmentMap.put(newId, server.activeSegment);

        // TODO : start compaction schedular
        return server;

    }
    // locking required only for the functions called by the client 
    public boolean put(String key, String value){
        rwLock.writeLock().lock();
        try{
            if(activeSegment.getSize() > MAX_SEGMENT_SIZE){
                this.rotateSegment();
            }

            long timestamp = System.currentTimeMillis();
            Record record = new Record(key,value,timestamp);
            long valueOffset = activeSegment.append(record);

            KeyDirEntry entry = new KeyDirEntry(activeSegment.getFileId(),value.getBytes(StandardCharsets.UTF_8).length,valueOffset,timestamp);
            this.keyDir.put(key, entry);

            return true;
        }catch(IOException e){
            System.out.println(e);
            return false;
        }finally{
            rwLock.writeLock().unlock();
        }

    }
    public String get(String key) {
        rwLock.readLock().lock();
        try {
            KeyDirEntry entry = this.keyDir.get(key);
            if (entry == null) return null;

            Segment seg = segmentMap.get(entry.getFileId());  // lookup up the right segment to read from according to fileId
            if (seg == null) return null;

            return seg.read(entry.getValueOffset(), entry.getValueSize());
        } catch (IOException e) {
            return null;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public HashMap<String, String> getAll() {
        HashMap<String, String> result = new HashMap<>(); // save all keys-> values
        
        // With entrySet():
        for (Map.Entry<String, KeyDirEntry> e : keyDir.getAll()) {
            String key      = e.getKey();
            String value = this.get(key);
            //System.out.println("key: " + key);
            //System.out.println("value : " + value);
            if (value != null) result.put(key, value);
        }
        return result;
    }

    private void rotateSegment() throws IOException{

        activeSegment.setFull();
        // TODO: write to hintfile

        // create a new active segment
        String newId = "Seg_" + (segmentMap.size() + 1);
        activeSegment = new Segment(this.directoryPath, newId, true);
        segmentMap.put(newId, activeSegment);  // ← add it here too

    }

    public void close() {
        // TODO: close schedular

        this.rwLock.writeLock().lock();
        try {
            this.activeSegment.setFull();
            // HintFile.write(directoryPath, activeSegment.fileId, keyDir)
            for (Segment seg : segmentMap.values()) seg.close();
        }catch(IOException e){
            System.out.println("closing corrupted");
        }
        finally {
            this.rwLock.writeLock().unlock();
        }
    }
}