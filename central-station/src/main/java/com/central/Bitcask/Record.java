package com.central.Bitcask;

public class Record {
    private String key;
    private String Value;
    private long timestamp;
    //private final long sequence;     // monotonic counter - for ordering

    /* then changes applied my be 
        // in BitcaskServer
        private final AtomicLong sequenceNumber = new AtomicLong(0);

        // in BitcaskServer put()
        long timestamp = sequenceNumber.incrementAndGet();
        Record record = new Record(key, value, timestamp);
    */


    public Record(String key, String value, long timestamp) {
        this.key = key;
        this.timestamp = timestamp;
        this.Value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return Value;
    }

    public void setValue(String Value) {
        this.Value = Value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}