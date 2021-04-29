package org.heigit.ohsome.oshdb;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public interface OSHDBTemporal {

  long getEpochSecond();

  default boolean isBefore(OSHDBTemporal other) {
    return getEpochSecond() < other.getEpochSecond();
  }

  default boolean isAfter(OSHDBTemporal other) {
    return getEpochSecond() > other.getEpochSecond();
  }

  static int compare(OSHDBTemporal a, OSHDBTemporal b) {
    return Long.compare(a.getEpochSecond(), b.getEpochSecond());
  }

  /**
   * Converts the given {@code OSHDBTemporal} to an iso-date-time string.
   * @param temporal The {@code OSHDBTemporal} which should converted.
   * @return the iso-date-time string for the {@code OSHDBTemporal}
   */
  static String toIsoDateTime(OSHDBTemporal temporal) {
    ZonedDateTime zdt =
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(temporal.getEpochSecond()), ZoneOffset.UTC);
    return zdt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  }
}
