package com.example.Bitcask;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/*
    for every KeyDirEntry whose fileId matches this segment:
        write a row: [timestamp][keyLen][offset][valueSize][key]
    → save as seg_001.hint alongside seg_001.data
*/

class HintFile {

    // Called when a segment is sealed/marked as full
    // Writes a compact index alongside the .data file
    // the hint file saves rows of: [timestamp][keysz][value_sz][value_pos][key]
    static void CreateHintFile(Segment segment,KeyDir keyDir) throws IOException{ 
        // if entry in keyDir belong to segment , write it
        try (DataOutputStream outputSteam = new DataOutputStream(new FileOutputStream(segment.getFilePath()+".hint"))) {
            // if entry in keyDir belong to segment , write it
            for (Map.Entry<String, KeyDirEntry> e : keyDir.getAll()) {
                String mapFileId = e.getValue().getFileId();
                if(mapFileId == null ? segment.getFileId() == null : mapFileId.equals(segment.getFileId())){
                    byte[] key      = e.getKey().getBytes(StandardCharsets.UTF_8);
                    KeyDirEntry kdValue = e.getValue();
                    
                    outputSteam.writeLong(kdValue.getTimestamp());
                    outputSteam.writeInt(key.length);
                    outputSteam.writeInt(kdValue.getValueSize());
                    outputSteam.writeLong(kdValue.getValueOffset());
                    outputSteam.write(key);
                }
                
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

        Path dir = Paths.get(directoryPath);

        List<Path> hintFiles = Files.list(dir)
                .filter(p -> p.toString().endsWith(".data"))
                .sorted(Comparator.comparingInt(p ->
                        Integer.parseInt(
                                p.getFileName().toString()
                                        .replace("Seg_", "")
                                        .replace(".hint", "")
                        )
                ))
                .toList();
        
        for (Path f : hintFiles) {
            String filename  = f.getFileName().toString();
            String fileId = filename.replace(".hint", "");
            
            //  keydir = key + keydirEntry[fileId, valueSize , valueOffset,  timestamp]
            // saved hint rows: [timestamp][keysz][value_sz][value_pos][key]
            try (DataInputStream inputStream = new DataInputStream(new FileInputStream(filename))) {
                while(inputStream.available() > 0){
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
                    }
                }
            }
        }
        return reKeyDir;
        
        // (newer entries win — this handles the case where a key was updated across multiple segments)
    }
}
