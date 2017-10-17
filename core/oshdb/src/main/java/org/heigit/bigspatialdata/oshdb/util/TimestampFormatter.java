package org.heigit.bigspatialdata.oshdb.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TimestampFormatter {

  private static TimestampFormatter _instance;
  private final SimpleDateFormat _formatDate = new SimpleDateFormat("yyyy-MM-dd");
  private final SimpleDateFormat _formatIsoDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

  private TimestampFormatter() {
    this._formatDate.setTimeZone(TimeZone.getTimeZone("UTC"));
    this._formatIsoDateTime.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public static TimestampFormatter getInstance() {
    if (_instance == null) {
      _instance = new TimestampFormatter();
    }
    return _instance;
  }

  public String date(Date date) {
    return _formatDate.format(date);
  }

  public String isoDateTime(Date date) {
    return _formatIsoDateTime.format(date);
  }

  public String isoDateTime(long date) {
    return _formatIsoDateTime.format(date);
  }

  public String osmTimestamp(long osmTimestamp) {
    return _instance.isoDateTime(osmTimestamp * 1000);
  }
}
