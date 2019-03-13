package org.heigit.bigspatialdata.oshdb.tool.importer.util;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.ToIntFunction;

import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

public class StringToIdMappingImpl implements StringToIdMapping {

  final Int2IntMap uniqueStrings;
  final Object2IntMap<String> nonUniqueStrings;
  final ToIntFunction<String> hashFunction;
  
  private final long estimatedSize;

  public StringToIdMappingImpl(final Int2IntMap uniqueStrings, final Object2IntMap<String> nonUniqueStrings,ToIntFunction<String> hashFunction, long estimatedSize) {
    this.uniqueStrings = uniqueStrings;
    this.uniqueStrings.defaultReturnValue(-1);
    this.nonUniqueStrings = nonUniqueStrings;
    this.nonUniqueStrings.defaultReturnValue(-1);
    this.hashFunction = hashFunction;
    this.estimatedSize = estimatedSize;
  }

  public int getId(final String key) {
    final int hash =  hashFunction.applyAsInt(key);
    int value = uniqueStrings.get(hash);
    if (value == -1)
      value = nonUniqueStrings.getInt(key);
    return value;
  }

  public Int2IntMap getUnique() {
    return uniqueStrings;
  }

  public Object2IntMap<String> getNonUnique() {
    return nonUniqueStrings;
  }
 
  public static StringToIdMappingImpl load(String stringToIdMapping,ToIntFunction<String> hashFunction) throws FileNotFoundException, IOException {
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(stringToIdMapping)))) {
      return read(in,hashFunction);
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(uniqueStrings.size());
    for (Int2IntMap.Entry e : uniqueStrings.int2IntEntrySet()) {
      out.writeInt(e.getIntKey());
      out.writeInt(e.getIntValue());
    }
    out.writeInt(nonUniqueStrings.size());
    for (Object2IntMap.Entry<String> e : nonUniqueStrings.object2IntEntrySet()) {
      out.writeUTF(e.getKey());
      out.writeInt(e.getIntValue());
    }
  }

  public static StringToIdMappingImpl read(DataInput in, ToIntFunction<String> hashFunction) throws IOException {
    int size = in.readInt();
    final Int2IntMap uniqueKeys = new Int2IntAVLTreeMap();
    long estimatedSize = 0;
    for (int i = 0; i < size; i++) {
      uniqueKeys.put(in.readInt(), in.readInt());
      estimatedSize += SizeEstimator.avlTreeEntry() + 8;
    }
    size = in.readInt();
    final Object2IntMap<String> notUniqueKeys = new Object2IntAVLTreeMap<>();
    for (int i = 0; i < size; i++) {
      final String s = in.readUTF();
      notUniqueKeys.put(s, in.readInt());
      estimatedSize += SizeEstimator.estimatedSizeOfAVLEntryValue(s)+4;
    }

    return new StringToIdMappingImpl(uniqueKeys, notUniqueKeys,hashFunction, estimatedSize);
  }

  public long estimatedSize() {
    return estimatedSize;
  }
}
