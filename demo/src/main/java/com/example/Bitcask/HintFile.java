package com.example.Bitcask;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/*
    for every KeyDirEntry whose fileId matches this segment:
        write a row: [timestamp][keyLen][offset][valueSize][key]
    → save as seg_001.hint alongside seg_001.data
*/

class HintFile {

    // Called when a merged segment is sealed/marked as full
    // Writes a compact index alongside the .data file
    // the hint file saves rows of: [timestamp][keysz][value_sz][value_pos][key]
    static void CreateHintFile(Segment segment) throws IOException{ 

        // i have segment , take all data in it and save it in hint file
        try (DataOutputStream outputSteam = new DataOutputStream(new FileOutputStream(segment.getFilePath()+".hint"))) {
            
            for (SegmentEntry e : segment.scanAll()) {
                byte[] key      = e.getKey().getBytes(StandardCharsets.UTF_8);
                outputSteam.writeLong(e.getTimestamp());
                outputSteam.writeInt(key.length);
                outputSteam.writeInt(e.getValuesz());
                outputSteam.writeLong(e.getValueOffset());
                outputSteam.write(key);

            }

            outputSteam.flush();
        }
    
    }

    // Used only if a hint file is missing (e.g crash before hint was written)
    static void rebuildFromDataFile(Segment segment, KeyDir keyDir) throws IOException{
        // read data from .data file instead from .hint file
        for (SegmentEntry entry : segment.scanAll()) {
            KeyDirEntry existing = keyDir.get(entry.getKey());
            if (existing == null || entry.getTimestamp() > existing.getTimestamp()) {
                keyDir.put(entry.getKey(), new KeyDirEntry(
                    segment.getFileId(),
                    entry.getValuesz(),
                    entry.getValueOffset(),
                    entry.getTimestamp()
                ));
            }
        }
    }

    // Called on startup — reads all hint files to restore KeyDir
    // Returns a fully populated KeyDir without reading .data files
    static KeyDir rebuild(String directoryPath) throws IOException{

        KeyDir reKeyDir = new KeyDir();

        List<Path> hintFiles = Files.list(Paths.get(directoryPath))
        .filter(p -> p.toString().endsWith(".hint"))
        .toList(); 
        
        for (Path f : hintFiles) {
            String filename  = f.getFileName().toString();
            String fileId = filename.replace(".hint", "");
            
            //  keydir = key + keydirEntry[fileId, valueSize , valueOffset,  timestamp]
            // saved hint rows: [timestamp][keysz][value_sz][value_pos][key]
            try (DataInputStream inputStream =new DataInputStream(Files.newInputStream(f))) {
                while(inputStream.available() > 0){
                    System.out.println("Loading from hint...");
                    long timestamp = inputStream.readLong();
                    int keysz = inputStream.readInt();
                    int valuesz = inputStream.readInt();
                    long valueOffset = inputStream.readLong();
                    byte[] keyBytes    = new byte[keysz];
                    inputStream.readFully(keyBytes);

                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    KeyDirEntry existing = reKeyDir.get(key);       // check if the key was already saved before and save the latest timestamp of it
                    if (existing == null || timestamp > existing.getTimestamp()) {
                        reKeyDir.put(key, new KeyDirEntry(fileId, valuesz, valueOffset, timestamp));
                        System.out.println("Updating KeyDir with hint files...");
                        System.out.println(
                            "RECOVERED: key=" + key +
                            " file=" + fileId +
                            " offset=" + valueOffset
                        );
                    }
                }
            }
        }
        return reKeyDir;
        
        // (newer entries win — this handles the case where a key was updated across multiple segments)
    }

    static void deleteHintFile(Segment segment){
        String filepath = segment.getFilePath();
        System.out.println("Hint Filepath in Delete : " + filepath);
        File hintFile = new File(filepath +".hint");
        if (hintFile.exists()){
            boolean ok = hintFile.delete();
            if(!ok) throw new RuntimeException("deleting hint-file " + filepath + " failed");
        }
    }
}
