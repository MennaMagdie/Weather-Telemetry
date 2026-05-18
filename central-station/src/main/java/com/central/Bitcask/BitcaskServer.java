package com.central.Bitcask;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
Design Decisions are 
1) the scheduler compaction each how much time ?
2) the max segment size then remarked full = ?
3) hint files save what data for optimized saving and retreiving
4) when to save - fsync files , after each write OR at closing the server -aka- closing the segment?
*/


public class BitcaskServer{
    public final long MAX_BATCH_SIZE = 10000;  // 10k record
    public final long MAX_SEGMENT_SIZE = 64 * 1024 * 1024;  // 64 MB     // Design Decision
    String directoryPath;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantLock mergeLock = new ReentrantLock();

    HashMap<String,Segment> segmentMap;
    Segment activeSegment;
    KeyDir keyDir;      // index Map
    ScheduledExecutorService scheduler;

    private BitcaskServer(){}       // private constructor - accessed only via open method
    
    public static BitcaskServer open(String directoryPath) throws IOException{
        BitcaskServer server = new BitcaskServer();
        server.directoryPath = directoryPath;

        new File(server.directoryPath).mkdirs();  // create new directory if there isn't one

        server.segmentMap = new HashMap<>();        // could be changed to ordered datastructure?
        server.keyDir = new KeyDir();

        // populate the segmentMap from the datafiles I have - not hint files as hint files may not exist but data files is the actual data saved
        try (Stream<Path> stream = Files.list(Paths.get(directoryPath))) {
            List<Path> dataFiles = stream
                .filter(p -> p.toString().endsWith(".data"))
                .toList();
        
            for (Path f : dataFiles) {      // loading segmentMap with all data files in the directory
                String  fileId  = f.getFileName().toString().replace(".data", "");
                Segment seg     = new Segment(server.directoryPath, fileId, false);
                server.segmentMap.put(fileId, seg);
            }
        }  
        /* 
         probably just remove the old files , load all .hint files , if there are seg_X and no Seg_merged_x .data in the directory , then load it too
        */       
        // rebuilt index hashMap
        server.keyDir = HintFile.rebuild(server.directoryPath); // rebuild from all hint files in the directory
        
        // Fallback: for any segment missing a hint file, scan the original data file
        for (Segment seg : server.segmentMap.values()) {
            File hintFile = new File(server.directoryPath, seg.getFileId() + ".hint");
            if (!hintFile.exists()) {
                HintFile.rebuildFromDataFile(seg, server.keyDir);
            }
        }

        // String newId = "Seg_" + getNewTimestamp(); // <<<<----------- could be changed to timestamp too?
        
        String newId = "Seg_" + (server.segmentMap.size() + 1);

        server.activeSegment = new Segment(server.directoryPath,newId,true);
        server.segmentMap.put(newId, server.activeSegment);


        // Schedule compaction every 3 minutes  <<<<---- variable
        server.scheduler = Executors.newSingleThreadScheduledExecutor();
        server.scheduler.scheduleAtFixedRate(
            () -> {
                if (server.mergeLock.tryLock()) {  // skip if already merging
                    try {
                        server.merge();
                    } catch (IOException e) {
                        // e.printStackTrace();
                        throw new RuntimeException("Scheduler failed to merge");
                    } finally {
                        server.mergeLock.unlock();
                    }
                } else {
                    System.out.println("Merge still running, skipping this interval");
                }
            },
            1, 3, TimeUnit.MINUTES          // intialDelay: the first run delay , period: the periodic run time
        );
            
        return server;

    }
    
    private long getNewTimestamp(){     // for all the times to be consistent
        return System.nanoTime();
    }

    // locking required only for the functions called by the client 
    public boolean put(String key, String value){
        rwLock.writeLock().lock();
        try{
            if(activeSegment.getSize() > MAX_SEGMENT_SIZE){
                this.rotateDataSegment();
            }

            long timestamp = getNewTimestamp();
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

    // close data file (full segment) - then create new data file (new segment)
    private void rotateDataSegment() throws IOException{

        activeSegment.markAsFull();
        // HintFile.createHintFile(activeSegment, this.keyDir);        // replaced -> hint-file only after compaction

        // create a new active segment
        String newId = "Seg_" + (segmentMap.size() + 1);
        activeSegment = new Segment(this.directoryPath, newId, true);
        segmentMap.put(newId, activeSegment);

    }

    // Triggers the compaction process on the given directory 
    // iterates all immutable data files, keeps only latest values, writes merged data files and their companion hint files.
    // bitcask:merge(DirectoryName) // definition in paper
    public synchronized void merge() throws IOException{

        // --- Phase 1: do all independent work without blocking reads/writes ---
        // 1) filter active segment from the to-compact segment
        List<Segment> oldSegments = this.segmentMap.values()
                                                .stream()                        
                                                .filter(s -> !s.isActive())      
                                                .collect(Collectors.toList());

        if(oldSegments.isEmpty()) return;  // empty           
         
        String fileId = "Seg_merged_" + getNewTimestamp();     // <-----------
        Segment currentMergedSegment = new Segment(this.directoryPath, fileId , true); // the new segment creation

        // collect latest version of each key accross all
        Map<String,SegmentEntry> latestEntries = new HashMap<>();

        for(Segment segment : oldSegments){
            for(SegmentEntry entry : segment.scanAll()){
                // System.out.println("SCAN: key=" + entry.getKey() + " ts=" + entry.getTimestamp() +" file=" + entry.getFileId());
                SegmentEntry exists = latestEntries.get(entry.getKey());
                if(exists == null || entry.getTimestamp() > exists.getTimestamp()){
                    latestEntries.put(entry.getKey(), entry);   // note entry contains key too
                    // System.out.println("LATEST UPDATE: " + entry.getKey() + " -> ts=" + entry.getTimestamp());
                }
            }
        }

        List<Segment> mergedSegments = new ArrayList<>();
        HashMap<String,KeyDirEntry> MergedKeyDirMap = new HashMap<>(); 

        for(SegmentEntry entry : latestEntries.values()){

            if(currentMergedSegment.getSize() > this.MAX_SEGMENT_SIZE){
                currentMergedSegment = this.rotateMergedSegment(currentMergedSegment, true, mergedSegments); // returns newMergedSegment
            }

            

            // populate mergedSegment with the entries
            Segment source = segmentMap.get(entry.getFileId());      // does this need a readlock?
            String value = source.read(entry.getValueOffset(), entry.getValuesz());
            //System.out.println("MERGING: key=" + entry.getKey() +" value=" + value +" from=" + entry.getFileId());

            Record record = new Record(entry.getKey(),value,entry.getTimestamp());
            long valueOffset = currentMergedSegment.append(record);
            // System.out.println("MERGED OFFSET: " + valueOffset +" into " + currentMergedSegment.getFileId());

            // update to keyDir
            MergedKeyDirMap.put(entry.getKey(), new KeyDirEntry(currentMergedSegment.getFileId(), entry.getValuesz(), valueOffset, entry.getTimestamp()));
        
            // System.out.println("KEYDIR UPDATE: " + entry.getKey() +" -> file=" + currentMergedSegment.getFileId() +" offset=" + valueOffset);
        }

        // close the open segment
        this.rotateMergedSegment(currentMergedSegment, false,mergedSegments); // returns newMergedSegment

        // --- Phase 2: brief write lock to swap server state atomically ---
        rwLock.writeLock().lock();
        try {
            // update keyDir with new locations
            for (Map.Entry<String, KeyDirEntry> entry : MergedKeyDirMap.entrySet()) {

                String key = entry.getKey();
                KeyDirEntry value = entry.getValue();
                this.keyDir.put(key,value);

            }
            // swap segmentMap — remove old, add merged
            for (Segment old : oldSegments) {
                segmentMap.remove(old.getFileId());
            }
            for (Segment merged : mergedSegments) {
                segmentMap.put(merged.getFileId(), merged);
            }
        } finally {
            rwLock.writeLock().unlock();
        }

        // delete oldSegments descriptors after release lock
        for(Segment old : oldSegments){ 
            // old.close();     // already handled in delete
            old.delete();       // deletes the data file
            HintFile.deleteHintFile(old);       // deletes corresponding hint file if exist
        }

    }

    private Segment rotateMergedSegment(Segment fullMergedSegment, boolean newSegment, List<Segment> mergedSegments) throws IOException{
        // sync and close the merged segment
        fullMergedSegment.markAsFull();
        fullMergedSegment.sync();   // can be delegated to the last close()
        // this.segmentMap.put(fullMergedSegment.getFileId(), fullMergedSegment);
        mergedSegments.add(fullMergedSegment);

        // create hint file for it after it is closed       
        try {
            HintFile.createHintFile(fullMergedSegment); // TODO: can be running on different thread too?
        } catch (IOException e) {
            // e.printStackTrace();
            throw new RuntimeException("Hint creation failed", e);
        }

        // fullMergedSegment.close();

        // create new Segment
        if(newSegment){
            String fileId = "Seg_merged_" + getNewTimestamp();     // <-----------
            Segment newMergedSegment = new Segment(this.directoryPath, fileId , true); // the new segment creation
            return newMergedSegment;
        }

        return null;
    }

    // called either after each write - then or only at close only (the implemented approach) [except for merged files , done on the spot]
    private void sync(){
        try{
            for (Segment seg : segmentMap.values()) seg.sync();  
        }catch(Exception e){
            // e.printStackTrace();
            throw new RuntimeException("Bitcask Sync Failed - data unsafe", e);
        }
    }

    public void close() {
        // close scheduler before lock
        this.scheduler.shutdown();
        
        try {
            // wait for running merge to finish
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }

        this.rwLock.writeLock().lock();
        try {
            this.activeSegment.markAsFull();
            this.sync(); //     <-- sync at close , NOTE : can also be sync at every write => slower but more durable
            HintFile.createHintFile(this.activeSegment);    // <---- do i add this or ignore the active segment and wait till the next merge?
            for (Segment seg : segmentMap.values()) seg.close();        // close the data files descriptors
        }catch(IOException e){
            throw new RuntimeException("Closing BitCask Server Failed", e);
        }
        finally {
            this.rwLock.writeLock().unlock();
        }
    }
}