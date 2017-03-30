package org.heigit.bigspatialdata.hosmdb.osh.builder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.heigit.bigspatialdata.hosmdb.osm.OSMEntity;
import org.heigit.bigspatialdata.hosmdb.osm.OSMWay;
import org.heigit.bigspatialdata.hosmdb.util.ByteArrayOutputWrapper;

public class Builder {
  
  private static final int CHANGED_USER_ID = 1 << 0;
  private static final int CHANGED_TAGS = 1 << 1;
  private static final int CHANGED_MEMBERS = 1 << 3;
  
  private final ByteArrayOutputWrapper output;
  private final long baseId;
  private final long baseTimestamp;
  private final long baseLongitude;
  private final long baseLatitude;
  
  
  int lastVersion = 0;
  long lastTimestamp = 0;
  long lastChangeset = 0;
  int lastUserId = 0;
  int[] lastKeyValues = new int[0];
  
  SortedSet<Integer> keySet = new TreeSet<>();
  
  boolean firstVersion = true;
  boolean timestampsNotInOrder = false;
  
  public Builder(final ByteArrayOutputWrapper output,final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude){
    this.output = output;
    this.baseId = baseId;
    this.baseTimestamp = baseTimestamp;
    this.baseLongitude = baseLongitude;
    this.baseLatitude = baseLatitude;
  }
  
  public boolean getTimestampsNotInOrder(){
    return timestampsNotInOrder;
  }
  
  public SortedSet<Integer> getKeySet(){
    return keySet;
  }
 
   public void build(OSMEntity version, byte changed) throws IOException{
    int v = (version.getVersion()* (!version.isVisible() ? -1 : 1)) - lastVersion; 
    output.writeSInt32(v);
    lastVersion = v;
    
    output.writeSInt64((version.getTimestamp() - lastTimestamp) - baseTimestamp);
    if (!firstVersion && lastTimestamp < version.getTimestamp())
      timestampsNotInOrder = true;
    lastTimestamp = version.getTimestamp();
    
    output.writeSInt64(version.getChangeset() - lastChangeset);
    lastChangeset = version.getChangeset();
    
    int userId = version.getUserId();
    if (userId != lastUserId)
      changed |= CHANGED_USER_ID;

    int[] keyValues = version.getTags();

    if (version.isVisible() && !Arrays.equals(keyValues, lastKeyValues)) {
      changed |= CHANGED_TAGS;
    }
    
    output.writeByte(changed);

    if ((changed & CHANGED_USER_ID) != 0) {
      output.writeSInt32(userId - lastUserId);
      lastUserId = userId;
    }

    if ((changed & CHANGED_TAGS) != 0) {
      output.writeUInt32(keyValues.length);
      for (int kv = 0; kv < keyValues.length; kv++) {
        output.writeUInt32(keyValues[kv]);
        if(kv%2 == 0) // only keys
          keySet.add(Integer.valueOf(keyValues[kv]));
      }
      lastKeyValues = keyValues;
    }
    
    firstVersion = false;
  }
  
}
