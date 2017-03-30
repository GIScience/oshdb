package org.heigit.bigspatialdata.hosmdb.osh;

import java.io.IOException;

public abstract class HOSM<T> {

  
  
  public abstract T compact(long baseId, long baseTimestamp, long baseLongitude,
      long baseLatitude) throws IOException;
  
  public abstract byte[] getData();
}
