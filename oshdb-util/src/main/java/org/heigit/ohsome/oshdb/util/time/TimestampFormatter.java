package org.heigit.ohsome.oshdb.util.time;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;

/**
 * A helper class to transform timestamps between string, date and long.
 *
 */
public class TimestampFormatter {

  private static TimestampFormatter instance;
  private final ThreadLocal<SimpleDateFormat> formatDate = ThreadLocal.withInitial(() -> {
    SimpleDateFormat ret = new SimpleDateFormat("yyyy-MM-dd");
    ret.setTimeZone(TimeZone.getTimeZone("UTC"));
    return ret;
  });
  private final ThreadLocal<SimpleDateFormat> formatIsoDateTime = ThreadLocal.withInitial(() -> {
    SimpleDateFormat ret = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    ret.setTimeZone(TimeZone.getTimeZone("UTC"));
    return ret;
  });

  /**
   * Get a standard TimestampFormatter.
   *
   * @return a timestamp formatter object
   */
  public static TimestampFormatter getInstance() {
    if (instance == null) {
      instance = new TimestampFormatter();
    }
    return instance;
  }

  /**
   * Converts a date to the format {@code yyyy-MM-dd}.
   *
   * @param date the date to be printed
   * @return the formatted date string
   */
  public String date(Date date) {
    return formatDate.get().format(date);
  }

  /**
   * Converts a date to the format {@code yyyy-MM-dd'T'HH:mm:ss'Z'}.
   *
   * @param date the date to be printed
   * @return the formatted date string
   */
  public String isoDateTime(Date date) {
    return formatIsoDateTime.get().format(date);
  }

  /**
   * Converts a unix-timestamp (oshdb-timestamp) to the format
   * {@code yyyy-MM-dd'T'HH:mm:ss'Z'} (OSM-Timestamp).
   *
   * @param timestamp the timestamp to be printed
   * @return the formatted date string
   */
  public String isoDateTime(long timestamp) {
    return formatIsoDateTime.get().format(timestamp * 1000);
  }

  /**
   * Converts a date to the format {@code yyyy-MM-dd'T'HH:mm:ss'Z'}.
   *
   * @param date the date to be printed
   * @return the formatted date string
   */
  public String isoDateTime(OSHDBTimestamp date) {
    return this.isoDateTime(date.getRawUnixTimestamp());
  }

}
