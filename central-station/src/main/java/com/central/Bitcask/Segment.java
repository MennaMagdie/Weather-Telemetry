package com.example.Bitcask;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

public class Segment{
    // needed to create a segment , append to segment , read from segment , close segment
    // String filename;
    //private File dataFile;
    private final RandomAccessFile logFile;
    private String fileId;
    private long curentOffset;
    private boolean isActive;
    // boolean isNew;

    public Segment(String directoryPath, String fileId, Boolean isNew) throws IOException{
        //this.filename = filename;
        this.fileId = fileId;
        String filename = directoryPath + "\\" + fileId + ".data";
        this.logFile = new RandomAccessFile(filename, "rw");
        // this.isNew = isNew;
        if(isNew){
            this.curentOffset = 0; // set offset to 0 if it's a new file
        }else{
            this.curentOffset = this.logFile.length(); // set offset to end of file if it's not new
        }
        // this.curentOffset = 0;        // adapts to open on current file size if file isn't new
        this.isActive = true;
    }

    public String read(long valueOffset, int valueSize) throws IOException {
        logFile.seek(valueOffset);
        byte[] valueBytes = new byte[valueSize];
        logFile.readFully(valueBytes);      // can be read() too but readFully() handles if buffer isn't filled
        return new String(valueBytes, StandardCharsets.UTF_8);
    }

    public long append(Record record) throws IOException {
        
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

    public void close() throws IOException{
        this.logFile.close(); // this closes the file descriptor itself
    }

    public void setFull(){
        this.isActive = false;  // used to mark the file as complete
    }

    public long getSize(){ 
        return this.curentOffset;
    }

    public boolean isActive() {
        return isActive;
    }

    public String getFileId() {
        return fileId;
    }
}
