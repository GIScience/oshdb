package org.heigit.bigspatialdata.hosmdb.util;

import java.io.IOException;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMEntity;

public interface Compactable <T extends HOSMEntity> {
  public abstract T compact(long baseId, long baseTimestamp, long baseLongitude,
      long baseLatitude) throws IOException;
}
