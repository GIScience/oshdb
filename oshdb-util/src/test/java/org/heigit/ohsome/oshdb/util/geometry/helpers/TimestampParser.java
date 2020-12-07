package org.heigit.ohsome.oshdb.util.geometry.helpers;

import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.time.IsoDateTimeParser;

public class TimestampParser {

  /**
   * Returns an {@link OSHDBTimestamp} converted with
   * {@link IsoDateTimeParser#parseIsoDateTime(String)} using a given {@link String timeString}.
   */
  public static OSHDBTimestamp toOSHDBTimestamp(String timeString) {
    return new OSHDBTimestamp(IsoDateTimeParser.parseIsoDateTime(timeString).toEpochSecond());
  }
}
