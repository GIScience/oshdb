package org.heigit.ohsome.oshdb.util.geometry.helpers;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.time.IsoDateTimeParser;

/**
 * Helper methods in addition to the {@link IsoDateTimeParser} class.
 */
public class TimestampParser {
  /**
   * Returns an {@link OSHDBTimestamp} converted with
   * {@link IsoDateTimeParser#parseIsoDateTime(String)} using a given {@link String timeString}.
   */
  public static OSHDBTimestamp toOSHDBTimestamp(String timeString) {
    return new OSHDBTimestamp(IsoDateTimeParser.parseIsoDateTime(timeString).toEpochSecond());
  }
}
