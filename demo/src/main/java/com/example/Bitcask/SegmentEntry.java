package com.example.Bitcask;

public class SegmentEntry {
    
   private final long timestamp;
   private final int keysz;
   private final int valuesz;
   private final String key;
   private final long valueOffset;

    public SegmentEntry(long timestamp, int keysz, int valuesz, String key , long valueOffset) {
        this.key = key;
        this.keysz = keysz;
        this.timestamp = timestamp;
        this.valueOffset = valueOffset;
        this.valuesz = valuesz;
    }

    public long getValueOffset() {
        return valueOffset;
    }

    public String getKey() {
        return key;
    }

    public int getValuesz() {
        return valuesz;
    }

    public int getKeysz() {
        return keysz;
    }

    public long getTimestamp() {
        return timestamp;
    }
}