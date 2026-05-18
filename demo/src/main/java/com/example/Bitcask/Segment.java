package com.example.Bitcask;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Segment{
    // needed to create a segment , append to segment , read from segment , close segment
    // String filename;
    private final String filePath;
    private final RandomAccessFile logFile;
    private final String fileId;
    private long curentOffset;
    private boolean isActive = false;
    // boolean isNew;
    private boolean isClosed = false;

    public Segment(String directoryPath, String fileId, Boolean isNew) throws IOException{
        //this.filename = filename;
        this.fileId = fileId;
        //this.filePath =  directoryPath + "\\" + fileId;
        this.filePath = Paths.get(directoryPath, fileId).toString();
        String dataFilename = this.filePath + ".data";
        // System.out.println("segment file name: " + dataFilename );

        this.logFile = new RandomAccessFile(dataFilename, "rw");
        // this.isNew = isNew;
        if(isNew){
            this.curentOffset = 0; // set offset to 0 if it's a new file
            this.isActive = true;
        }else{
            this.curentOffset = this.logFile.length(); // set offset to end of file if it's not new
        }
    }

    public String read(long valueOffset, int valueSize) throws IOException {
        if(isClosed) {
            System.err.println("file is closed on read ??");
            return null;
        }
        logFile.seek(valueOffset);
        byte[] valueBytes = new byte[valueSize];
        logFile.readFully(valueBytes);      // can be read() too but readFully() handles if buffer isn't filled
        return new String(valueBytes, StandardCharsets.UTF_8);
    }

    public long append(Record record) throws IOException {

        if(this.isClosed) {
            System.err.println("file is closed on append ??");
        }
        
        byte[] key = record.getKey().getBytes(StandardCharsets.UTF_8);
        byte[] value = record.getValue().getBytes(StandardCharsets.UTF_8);
        long timestamp = record.getTimestamp();

        // Order in segment [timestamp = 8 bytes][keylen = 4 bytes][valueLen = 4 bytes][key][value] according to bitcask paper
        int keySize = key.length;
        int valueSize = value.length;
        // long valueOffset = this.curentOffset + Long.BYTES + Integer.BYTES*2 + keySize;

        this.logFile.seek(this.curentOffset);
        //System.out.println("currentoffset before :" + this.curentOffset);
        this.logFile.writeLong(timestamp);
        this.logFile.writeInt(keySize);
        this.logFile.writeInt(valueSize);
        this.logFile.write(key);

        long valueOffset = logFile.getFilePointer();
        this.logFile.write(value);
        
        // then update offset 
        //this.curentOffset += Long.BYTES + Integer.BYTES*2 + keySize + valueSize; // for next record append
        this.curentOffset = logFile.getFilePointer();

        //System.out.println("currentoffset after :" + this.curentOffset);

        return valueOffset;  // returns valueOffset of the append that would be written in indexMap for fast retrieval of the value
    }

    // to scan all the entries for the fallback reading from .data files if no .hint file exist at recovery
    public List<SegmentEntry> scanAll() throws IOException{

        if(this.isActive) System.out.println("scanning while segment is active, this maybe not correct !!");

        List<SegmentEntry> entries = new ArrayList<>();
        this.logFile.seek(0);    // start reading from the start

        // Order in segment [timestamp][keylen][valueLen][key][value]
        while(this.logFile.getFilePointer() < this.logFile.length()){
            long timestamp = this.logFile.readLong();
            int keysz = this.logFile.readInt();
            int valuesz = this.logFile.readInt();
            
            byte[] keyBytes = new byte[keysz];
            this.logFile.readFully(keyBytes);

            long valueOffset = this.logFile.getFilePointer();
            this.logFile.skipBytes(valuesz);        // don't care about value, need only value offset

            entries.add(new SegmentEntry(
                timestamp,
                keysz,
                valuesz,
                new String(keyBytes, StandardCharsets.UTF_8), 
                valueOffset,
                this.fileId
            ));
        }
        return entries;
    }

    // one of them is the correct one
    public void close() throws IOException{
        this.logFile.close(); // this closes the file descriptor itself
        this.isClosed = true;
    }

    public void sync(){
        if(this.isClosed){
            throw new RuntimeException("Sync is done on closed segment");
        }
        try {
            this.logFile.getFD().sync();    // this syncs and flushes to disk
        } catch (IOException e) {
            throw new RuntimeException("Segment durability failed", e);
        }
    }

    // must close first
    public void delete() throws IOException{
        if(!this.isClosed)  this.close();
        Files.delete(Paths.get(this.filePath + ".data"));
    }

    public void markAsFull(){
        this.isActive = false;  // used to mark the file as complete
    }

    public long getSize(){ 
        return this.curentOffset;
    }
    
    public String getFilePath() {
        return filePath;
    }

    public boolean isActive() {
        return isActive;
    }

    public String getFileId() {
        return fileId;
    }
}
