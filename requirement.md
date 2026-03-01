Version : 0.1
Create HBaseDatabase class which extends Database interface and dummy implement it's methods

Create BlockStore class which does below:
1. Accept maxActiveKeyCount during creation
2. Internally creates HashMap<K, V> and store key, value of put, get, scan and delete operation and also expose isFull method


Version : 0.2
1. Change V to class of Value which contains V value, boolean isDeleted;
Create SST class which will contain List of BlockStore from latest to oldest
It has below methods:
Constructor: Create Stack<BlockStore> with first BlockStore as sentinel value with static max key per block. 
1. Get(key) : blockList.forEach(a -> a.get(key) if found return and stop future lookup)
2. PUT(Key, value) : blockList.getLatest() blockStore and do put in it. 
3. Scan(String prefix) : blockList.forEach(a -> a.scan(key)).collection List of results
4. Delete(Key) : blockList.forEach(a -> a.get(key) if found mark field key as deleted and return and stop future lookup)


Version 0.3
Add ListerClass which get trigger when we have available `BLockerStore` which is full and has not been persisted yet

We get all the entries from the `BLockerStore` in sorted string order and write to file.
While writing to File we write as below
1. Sorted string keys => Value data\n
2. This will help us search faster on the file itself.

Post that we will create smaller TreeMap<Key, FilePointer> so we can get do prefix search and query file using File Pointer.

This class will be named as BackupStore taking BlockStore and onca back is completed BlockStore(isBackedUp = true)
And replace full map with indexMap and if isBackedUp = we empty the original map to reduce memmory and use fileIndex with Map<String, FilePointer> as given.

Backup process is completely async and will not impact ongoing read / write.