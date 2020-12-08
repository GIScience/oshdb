package org.heigit.bigspatialdata.oshdb.util.geometry.helpers;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.IsoDateTimeParser;

public class TimestampParser {

  /**
   * Returns an {@link OSHDBTimestamp} converted with
   * {@link IsoDateTimeParser#parseIsoDateTime(String)} using a given {@link String timeString}.
   */
  public static OSHDBTimestamp toOSHDBTimestamp(String timeString) {
    return new OSHDBTimestamp(IsoDateTimeParser.parseIsoDateTime(timeString).toEpochSecond());
  }
}
