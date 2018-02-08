package org.heigit.bigspatialdata.oshdb.osm2;

import java.util.Arrays;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public interface OSMEntity {

  public long getId();
  public boolean isVisible();
  
  public int getVersion();
  public OSHDBTimestamp getTimestamp();
  public long getChangeset();
  public int getUserId();
  public int[] getTags();
 

  public default String asString() {
    return String.format("ID:%d V:%d TS:%d CS:%d VIS:%s UID:%d TAGS:%S", getId(), getVersion(),
        getTimestamp().getRawUnixTimestamp(), getChangeset(), isVisible(), getUserId(), Arrays.toString(getTags()));
  }
}
