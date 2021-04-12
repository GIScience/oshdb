package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import java.util.Arrays;
import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;

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
        getTimestamp().getEpochSecond(), getChangeset(), isVisible(), getUserId(), Arrays.toString(getTags()));
  }
}
