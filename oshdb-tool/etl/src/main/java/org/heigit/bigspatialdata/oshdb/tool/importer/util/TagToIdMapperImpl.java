package org.heigit.bigspatialdata.oshdb.tool.importer.util;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.ToIntFunction;

public class TagToIdMapperImpl implements TagToIdMapper{

  private final StringToIdMappingImpl keyToId;
  private final StringToIdMappingImpl[] valueToId;

  public TagToIdMapperImpl(final StringToIdMappingImpl keyToId, final StringToIdMappingImpl[] valueToId) {
    this.keyToId = keyToId;
    this.valueToId = valueToId;
  }

  
  public void get(String key, String value, int[] kv, int offset){
    final int keyId = keyToId.getId(key);
    kv[offset] = keyId;
    kv[offset+1] = valueToId[keyId].getId(value);
  }
  
  
  public int[] get(String key, String value) {
    final int[] ret = new int[2];
    get(key,value, ret,0);
    return ret;
  }
  
 
  public static TagToIdMapperImpl load(String kvToIdMapping,ToIntFunction<String> hashFunction) throws FileNotFoundException, IOException {
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(kvToIdMapping)))) {
      return read(in,hashFunction);
    }
  }

  public void write(DataOutput out) throws IOException {
    keyToId.write(out);
    out.writeInt(valueToId.length);
    for (StringToIdMappingImpl value : valueToId) {
      value.write(out);
    }
  }

  public static TagToIdMapperImpl read(DataInput in,ToIntFunction<String> hashFunction) throws IOException{
    StringToIdMappingImpl keyToId = StringToIdMappingImpl.read(in,hashFunction);
    StringToIdMappingImpl[] valueToId = new StringToIdMappingImpl[in.readInt()];
    for (int i = 0, iL = valueToId.length; i < iL; i++) {
      valueToId[i] = StringToIdMappingImpl.read(in,hashFunction);
    }
    return new TagToIdMapperImpl(keyToId, valueToId);
  }


  @Override
  public int getKey(String key) {
    return keyToId.getId(key);
  }


  @Override
  public int getValue(int key, String value) {
    return valueToId[key].getId(value);
  }


  @Override
  public TagId getTag(String key, String value) {
    final int keyId = getKey(key);
    if(keyId == -1)
      return null;
    final int valueId = getValue(keyId,value);
    if(valueId == -1)
      return null;
    return TagId.of(keyId,valueId);
  }


  @Override
  public long estimatedSize() {
    long size = keyToId.estimatedSize();
    for(StringToIdMappingImpl value : valueToId)
      size += value.estimatedSize();
    return size;
  }

}
