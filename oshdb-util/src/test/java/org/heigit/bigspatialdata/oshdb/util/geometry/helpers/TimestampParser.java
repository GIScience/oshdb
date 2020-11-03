package org.heigit.bigspatialdata.oshdb.util.geometry.helpers;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.IsoDateTimeParser;

public class TimestampParser {
  public static OSHDBTimestamp toOSHDBTimestamp(String timeString) {
    try {
      return new OSHDBTimestamp(
          IsoDateTimeParser.parseIsoDateTime(timeString).toEpochSecond()
      );
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}
