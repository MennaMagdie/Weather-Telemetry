- main.java is currently a testclass for bitcask 
### output :
![alt text](./images/test_output.png)

- Example structure of the project
``` 
demo/
  pom.xml
  src/main/java/
    bitcask/
      BitCask.java      ✓  but to modified yet too with compaction and hintfile
      KeyDir.java       ✓
      Segment.java      ✓
      HintFile.java     X
      Compactor.java    X
    kafka/
      WeatherConsumer.java
      RainProcessor.java
    parquet/
      ParquetWriter.java
    api/
      BitCaskController.java   ← REST endpoints for the client script
```

### the bitcask access methods :
* get(key) -> gets the value of given key (should return latest value of key (station))
* put(key,value,timestamp) -> creates a record with the data and insert it in the segment
* getAll() -> to get *all* stations latest value

# According to Bitcask paper

## Hintfiles:

### content:

They're structured almost identically to the regular data files, but with one key difference:
instead of storing the actual values, they store the position and size of where each value lives within the corresponding data file.
So a hint file entry has fields like `tstamp|ksz|value_sz|value_pos|key` - but no value itself.

The reason:
When a Bitcask instance is opened, it needs to reconstruct the in-memory keydir (the hash table mapping every key to its file location).
Without hint files, it would have to scan through every data file entirely to rebuild this structure, which is slow. With hint files, it can scan those instead, they're much smaller since they omit all the value data, making startup significantly faster.

if no hint file is found for a data file:
The reconstruction scans all of the data files in the directory in order to build a new keydir.
This means it reads through every single data file entry by entry - going through the full `crc | tstamp | ksz | value_sz | key | value records` - just to extract the key locations.
This also works but is significantly slower since it's reading through all the value bytes too, even though it doesn't need them for rebuilding the keydir, hence the purpose for hint files for fast recovery.


### Note:
hint files are not a snapshot of the keydir saved on close. They are tied specifically to the merge process. Here's the distinction:

The merge process takes multiple older immutable data files and compacts them into a new merged data file per segment
A hint file is created alongside each output merged data file, acting as its pre-built index
The active data file (the one currently being written to) never has a hint file — it hasn't been through a merge yet

So on startup, Bitcask handles files in two different ways:
1) Merged segments → use their hint file for fast keydir reconstruction
2) The active file (and any unmerged segments) → must be scanned directly - hence why on close , the active segment must be "marked as full" even though its not to seal it - save it - merge it and create hint-file for it 