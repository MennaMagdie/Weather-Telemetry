// package com.example.Bitcask;
// import java.io.File;
// import java.io.IOException;
// import java.nio.charset.StandardCharsets;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.concurrent.Executors;
// import java.util.concurrent.ScheduledExecutorService;
// import java.util.concurrent.TimeUnit;
// import java.util.concurrent.locks.ReentrantLock;
// import java.util.concurrent.locks.ReentrantReadWriteLock;
// import java.util.stream.Collectors;
// import java.util.stream.Stream;

// /*
// Design Decisions are 
// 1) the schedular compaction each how much time ?
// 2) the max segment size then remarked full = ?
// 3) hint files save what data for optimized saving and retreiving
// 4) when to save - fsync files , after each write OR at closing the server -aka- closing the segment?
// */


// public class BitcaskServer{
//     public final long MAX_BATCH_SIZE = 10000;  // 10k record
//     public final long MAX_SEGMENT_SIZE = 64 * 1024 * 1024;  // 64 MB     // Design Decision
//     String directoryPath;

//     private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
//     private final ReentrantLock mergeLock = new ReentrantLock();

//     HashMap<String,Segment> segmentMap;
//     Segment activeSegment;
//     KeyDir keyDir;      // index Map
//     ScheduledExecutorService scheduler;

//     private BitcaskServer(){}       // private constructor - accessed only via open method
    
//     public static BitcaskServer open(String directoryPath) throws IOException{
//         BitcaskServer server = new BitcaskServer();
//         server.directoryPath = directoryPath;

//         new File(server.directoryPath).mkdirs();  // create new directory if there isn't one

//         server.segmentMap = new HashMap<>();        // could be changed to ordered datastructure?
//         server.keyDir = new KeyDir();

//         // populate the segmentMap from the datafiles I have - not hint files as hint files may not exist but data files is the actual data saved
//         try (Stream<Path> stream = Files.list(Paths.get(directoryPath))) {
//             List<Path> dataFiles = stream
//                 .filter(p -> p.toString().endsWith(".data"))
//                 .toList();
        
//             for (Path f : dataFiles) {      // loading segmentMap with all data files in the directory
//                 String  fileId  = f.getFileName().toString().replace(".data", "");
//                 Segment seg     = new Segment(server.directoryPath, fileId, false);
//                 server.segmentMap.put(fileId, seg);
//             }
//         }  
//         /* 
//          probably just remove the old files , load all .hint files , if there are seg_X and no Seg_merged_x .data in the directory , then load it too
//         */       
//         // rebuilt index hashMap
//         server.keyDir = HintFile.rebuild(server.directoryPath); // rebuild from all hint files in the directory
        
//         // Fallback: for any segment missing a hint file, scan the original data file
//         for (Segment seg : server.segmentMap.values()) {
//             File hintFile = new File(server.directoryPath, seg.getFileId() + ".hint");
//             if (!hintFile.exists()) {
//                 HintFile.rebuildFromDataFile(seg, server.keyDir);
//             }
//         }

//         String newId = "Seg_" + System.currentTimeMillis(); // <<<<----------- could be changed to timestamp too?
        
//         server.activeSegment = new Segment(server.directoryPath,newId,true);
//         server.segmentMap.put(newId, server.activeSegment);


//         // Schedule compaction every 5 minutes  <<<<---- variable
//         server.scheduler = Executors.newSingleThreadScheduledExecutor();
//         server.scheduler.scheduleAtFixedRate(
//             () -> {
//                 try {
//                     server.merge();
//                 } catch (IOException e) {
//                     System.err.println("schedular failed !!");
//                     e.printStackTrace();
//                 }
//             },
//             5, 5, TimeUnit.MINUTES
//         );
            
//         return server;

//     }
//     // locking required only for the functions called by the client 
//     public boolean put(String key, String value){
//         rwLock.writeLock().lock();
//         try{
//             if(activeSegment.getSize() > MAX_SEGMENT_SIZE){
//                 this.rotateDataSegment();
//             }

//             long timestamp = System.currentTimeMillis();
//             Record record = new Record(key,value,timestamp);
//             long valueOffset = activeSegment.append(record);

//             KeyDirEntry entry = new KeyDirEntry(activeSegment.getFileId(),value.getBytes(StandardCharsets.UTF_8).length,valueOffset,timestamp);
//             this.keyDir.put(key, entry);

//             return true;
//         }catch(IOException e){
//             System.out.println(e);
//             return false;
//         }finally{
//             rwLock.writeLock().unlock();
//         }

//     }
//     public String get(String key) {
//         rwLock.readLock().lock();
//         try {
//             KeyDirEntry entry = this.keyDir.get(key);
//             if (entry == null) return null;

//             Segment seg = segmentMap.get(entry.getFileId());  // lookup up the right segment to read from according to fileId
//             if (seg == null) return null;

//             return seg.read(entry.getValueOffset(), entry.getValueSize());
//         } catch (IOException e) {
//             return null;
//         } finally {
//             rwLock.readLock().unlock();
//         }
//     }

//     public HashMap<String, String> getAll() {
//         HashMap<String, String> result = new HashMap<>(); // save all keys-> values
        
//         // With entrySet():
//         for (Map.Entry<String, KeyDirEntry> e : keyDir.getAll()) {
//             String key      = e.getKey();
//             String value = this.get(key);
//             //System.out.println("key: " + key);
//             //System.out.println("value : " + value);
//             if (value != null) result.put(key, value);
//         }
//         return result;
//     }

//     // close data file (full segment) - then create new data file (new segment)
//     private void rotateDataSegment() throws IOException{

//         activeSegment.markAsFull();
//         // HintFile.CreateHintFile(activeSegment, this.keyDir);        // replaced -> hint-file only after compaction

//         // create a new active segment
//         String newId = "Seg_" + (segmentMap.size() + 1);
//         activeSegment = new Segment(this.directoryPath, newId, true);
//         segmentMap.put(newId, activeSegment);

//     }

//     // Triggers the compaction process on the given directory 
//     // iterates all immutable data files, keeps only latest values, writes merged data files and their companion hint files.
//     // bitcask:merge(DirectoryName) // definition in paper
//     private void merge() throws IOException{

//         // 1) filter active segment from the to-compact segment
//         List<Segment> oldSegments = this.segmentMap.values()
//                                                 .stream()                        
//                                                 .filter(s -> !s.isActive())      
//                                                 .collect(Collectors.toList());

//         if(oldSegments.isEmpty()) return;  // empty           
         
//         String fileId = "Seg_merged_" + System.currentTimeMillis();     // <-----------
//         Segment currentMergedSegment = new Segment(this.directoryPath, fileId , true); // the new segment creation

//         // collect latest version of each key accross all
//         Map<String,SegmentEntry> latestEntries = new HashMap<>();

//         for(Segment segment : oldSegments){
//             for(SegmentEntry entry : segment.scanAll()){
//                 SegmentEntry exists = latestEntries.get(entry.getKey());
//                 if(exists == null || entry.getTimestamp() > exists.getTimestamp()){
//                     latestEntries.put(entry.getKey(), entry);   // note entry contains key too
//                 }
//             }
//         }

//         for(SegmentEntry entry : latestEntries.values()){

//             if(currentMergedSegment.getSize() > this.MAX_SEGMENT_SIZE){
//                 currentMergedSegment = this.rotateMergedSegment(currentMergedSegment, true); // returns newMergedSegment
//             }

//             // populate mergedSegment with the entries
//             Segment source = segmentMap.get(entry.getFileId());
//             String value = source.read(entry.getValueOffset(), entry.getValuesz());
//             Record record = new Record(entry.getKey(),value,entry.getTimestamp());
//             long valueOffset = currentMergedSegment.append(record);

//             // update to keyDir
//             this.keyDir.put(entry.getKey(), new KeyDirEntry(currentMergedSegment.getFileId(), entry.getValuesz(), valueOffset, entry.getTimestamp()));
        
//         }

//         // close the open segment
//         this.rotateMergedSegment(currentMergedSegment, false); // returns newMergedSegment

//         // delete oldSegments
//         for(Segment old : oldSegments){
//             segmentMap.remove(old.getFileId());
//             // old.close();     // already handled in delete
//             old.delete();
//         }

//     }

//     private Segment rotateMergedSegment(Segment fullMergedSegment, boolean newSegment) throws IOException{
//         // sync and close the merged segment
//         fullMergedSegment.markAsFull();
//         fullMergedSegment.sync();
//         fullMergedSegment.close();
//         // this.segmentMap.put(fullMergedSegment.getFileId(), fullMergedSegment);
        

//         // create hint file for it after it is closed
//         HintFile.CreateHintFile(fullMergedSegment);         // can be running on different thread too?
//         // create new Segment
//         if(newSegment){
//             String fileId = "Seg_merged_" + System.currentTimeMillis();     // <-----------
//             Segment newMergedSegment = new Segment(this.directoryPath, fileId , true); // the new segment creation
//             return newMergedSegment;
//         }

//         return null;
//     }

//     // called either after each write - then or only at close
//     private void sync(){
//         try{
//             for (Segment seg : segmentMap.values()) seg.sync();  
//         }catch(Exception e){
//             System.err.println("Bitcask Sync Failed");
//         }
//     }

//     public void close() {
//         // close schedular before lock
//         this.scheduler.shutdown();
//         try {
//             // wait for running merge to finish
//             if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
//                 scheduler.shutdownNow();
//             }
//         } catch (InterruptedException e) {
//             scheduler.shutdownNow();
//         }
//         this.rwLock.writeLock().lock();
//         try {
//             this.activeSegment.markAsFull();
//             this.sync(); //     <-- sync at close , NOTE : can also be sync at every write => slower but more durable
//             for (Segment seg : segmentMap.values()) seg.close();        // close the data files descriptors
//         }catch(IOException e){
//             System.out.println("Closing BitCask Server Corrupted");
//         }
//         finally {
//             this.rwLock.writeLock().unlock();
//         }
//     }
// }
