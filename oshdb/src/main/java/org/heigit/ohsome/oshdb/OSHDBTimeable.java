package org.heigit.ohsome.oshdb;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public interface OSHDBTimeable {
  
  long getEpochSecond();
  
  default OSHDBTimestamp getTimestamp() {
    return new OSHDBTimestamp(getEpochSecond());
  }
  
  default boolean isBefore(OSHDBTimeable other) {
    return getEpochSecond() < other.getEpochSecond();
  }
  
  static boolean isBefore(OSHDBTimeable a, OSHDBTimeable b) {
    return a.isBefore(b);
  }
  
  default boolean isAfter(OSHDBTimeable other) {
    return getEpochSecond() > other.getEpochSecond();
  }
  
  static boolean isAfter(OSHDBTimeable a, OSHDBTimeable b) {
    return a.isAfter(b);
  }
  
  static int compare(OSHDBTimeable a, OSHDBTimeable b) {
    return Long.compare(a.getEpochSecond(), b.getEpochSecond());
  }
  
  static String toString(OSHDBTimeable o) {
    ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(o.getEpochSecond()), ZoneOffset.UTC);
    return zdt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  }
}
