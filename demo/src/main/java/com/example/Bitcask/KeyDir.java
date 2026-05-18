package com.example.Bitcask;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class KeyDir{
    
    private HashMap<String,KeyDirEntry> keyDirMap;   // the in-memory map index
    
    public KeyDir() {
        this.keyDirMap = new HashMap<>();  
    }

    void put(String key, KeyDirEntry entry){
        this.keyDirMap.put(key, entry);
    }

    KeyDirEntry get(String key){
        return this.keyDirMap.get(key);
    }


    boolean containsKey(String key){
        return this.keyDirMap.containsKey(key);
    }

    Set<HashMap.Entry<String, KeyDirEntry>> getAll() {      
        // return this.keyDirMap.entrySet();   // to return a live read only reference
        return new HashSet<>(this.keyDirMap.entrySet()); // returns a snapshot of the keyDirMap 
    }
}

