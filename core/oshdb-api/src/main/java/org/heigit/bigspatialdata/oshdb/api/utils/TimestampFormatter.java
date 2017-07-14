package org.heigit.bigspatialdata.oshdb.api.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampFormatter {
  private static TimestampFormatter _instance;
  private static final SimpleDateFormat _formatDate = new SimpleDateFormat("yyyy-MM-dd");
  private static final SimpleDateFormat _formatIsoDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

  private TimestampFormatter() {}

  public static TimestampFormatter getInstance() {
    if (_instance == null) _instance = new TimestampFormatter();
    return _instance;
  }

  public String date(Date date) {
    return _formatDate.format(date);
  }

  public String isoDateTime(Date date) {
    return _formatIsoDateTime.format(date);
  }
}
