package org.heigit.bigspatialdata.hosmdb.osh;


import java.io.IOException;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.heigit.bigspatialdata.hosmdb.osm.OSMEntity;
import org.heigit.bigspatialdata.hosmdb.util.BoundingBox;
import org.heigit.bigspatialdata.hosmdb.util.ByteArrayOutputWrapper;

public abstract class HOSMEntity<OSM extends OSMEntity> implements Comparable<HOSMEntity>,Iterable<OSM> {
  
  public static final int NODE = 0;
  public static final int WAY = 1;
  public static final int RELATION = 2;

  protected final byte[] data;
  protected final int offset;
  protected final int length;
  protected final long baseTimestamp;
  protected final long baseLongitude;
  protected final long baseLatitude;

  protected final long id;
  protected final byte header;
  protected final BoundingBox bbox;
  protected final int[] keys;
  protected final int dataOffset;
  protected final int dataLength;

  public HOSMEntity(final byte[] data, final int offset, final int length, 
      final long baseId,final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final byte header, final long id, final BoundingBox bbox, final int[] keys,
      final int dataOffset, final int dataLength) {
    this.data = data;
    this.offset = offset;
    this.length = length;

    this.baseTimestamp = baseTimestamp;
    this.baseLongitude = baseLongitude;
    this.baseLatitude = baseLatitude;
    
    this.header = header;
    this.id = id;
    this.bbox = bbox;
    this.keys = keys;
    this.dataOffset = dataOffset;
    this.dataLength = dataLength;
  }
  
  public byte[] getData() {
    if (offset == 0 && length == data.length)
      return data;
    byte[] result = new byte[length];
    System.arraycopy(data, offset, result, 0, length);
    return result;
  }
  
  public long getId() {
    return id;
  }

  public int getLength() {
    return length;
  }
  
  public BoundingBox getBoundingBox(){
    return bbox;
  }
  
  public int[] getKeys(){
    return keys;
  }
  
  public abstract List<OSM> getVersions();
  
  public Map<Long,OSM> getByTimestamps(List<Long> byTimestamps){
	  
	  Map<Long,OSM> result = new TreeMap<>();
	  if(byTimestamps == null || byTimestamps.isEmpty()){
		  Iterator<OSM> itr = iterator();
		  while(itr.hasNext()){
			  OSM osm = itr.next();
			  result.put(osm.getTimestamp(), osm);
		  }
		  return result;
	  }
	  
	  Collections.sort(byTimestamps,Collections.reverseOrder());
	  
	  int i = 0;
	  Iterator<OSM> itr = iterator();
	  while(itr.hasNext() && i > byTimestamps.size()){
		  OSM osm = itr.next();
		  if(osm.getTimestamp() > byTimestamps.get(i)){
			  continue;
		  } else {
			  result.put(byTimestamps.get(i++), osm);
		  }
	  }
	  
	  return result;
  }
  
  public boolean hasKey(int key){
    for(int i=0; i<keys.length; i++){
      if(keys[i] == key)
        return true;
    }
    return false;
  }
  
  public abstract HOSMEntity rebase(long baseId, long baseTimestamp, long baseLongitude,
	      long baseLatitude) throws IOException;

  @Override
  public int compareTo(HOSMEntity o) {
    int c = Long.compare(id, o.id);
    return c;
  }
  
  public void writeTo(ByteArrayOutputWrapper out) throws IOException {
    out.writeByteArray(data, offset, length);
  }
  
  public int writeTo(ObjectOutput out) throws IOException {
    out.write(data, offset, length);
    return length;
  }

}
