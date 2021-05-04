package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import java.util.Arrays;
import org.heigit.ohsome.oshdb.OSHDBTemporal;

public interface OSMEntity extends OSHDBTemporal {

  long getId();

  boolean isVisible();

  int getVersion();

  long getChangeset();

  int getUserId();

  int[] getTags();


  public default String asString() {
    return String.format("ID:%d V:%d TS:%d CS:%d VIS:%s UID:%d TAGS:%S", getId(), getVersion(),
        getEpochSecond(), getChangeset(), isVisible(), getUserId(),
        Arrays.toString(getTags()));
  }
}
