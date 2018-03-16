package org.heigit.bigspatialdata.oshdb.util.time;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

/**
 * A helper class to transform timestamps between string, date and long.
 *
 */
public class TimestampFormatter {

  private static TimestampFormatter _instance;
  private final ThreadLocal<SimpleDateFormat> _formatDate = ThreadLocal.withInitial(() -> {
    SimpleDateFormat ret = new SimpleDateFormat("yyyy-MM-dd");
    ret.setTimeZone(TimeZone.getTimeZone("UTC"));
    return ret;
  });
  private final ThreadLocal<SimpleDateFormat> _formatIsoDateTime = ThreadLocal.withInitial(() -> {
    SimpleDateFormat ret = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    ret.setTimeZone(TimeZone.getTimeZone("UTC"));
    return ret;
  });

  /**
   * Get a standard TimestampFormatter.
   *
   * @return
   */
  public static TimestampFormatter getInstance() {
    if (_instance == null) {
      _instance = new TimestampFormatter();
    }
    return _instance;
  }

  /**
   * Converts a date to the format {@code yyyy-MM-dd}.
   *
   * @param date
   * @return
   */
  public String date(Date date) {
    return _formatDate.get().format(date);
  }

  /**
   * Converts a date to the format {@code yyyy-MM-dd'T'HH:mm:ss'Z'}.
   *
   * @param date
   * @return
   */
  public String isoDateTime(Date date) {
    return _formatIsoDateTime.get().format(date);
  }

  /**
   * Converts a unix-timestamp (oshdb-timestamp) to the format
   * {@code yyyy-MM-dd'T'HH:mm:ss'Z'} (OSM-Timestamp).
   *
   * @param timestamp
   * @return
   */
  public String isoDateTime(long timestamp) {
    return _formatIsoDateTime.get().format(timestamp * 1000);
  }

  /**
   * Converts a date to the format {@code yyyy-MM-dd'T'HH:mm:ss'Z'}.
   *
   * @param date
   * @return
   */
  public String isoDateTime(OSHDBTimestamp date) {
    return this.isoDateTime(date.getRawUnixTimestamp());
  }

}
