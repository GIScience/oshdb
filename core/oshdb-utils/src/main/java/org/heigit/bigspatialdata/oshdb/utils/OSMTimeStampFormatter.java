package org.heigit.bigspatialdata.oshdb.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class OSMTimeStampFormatter {
  private static OSMTimeStampFormatter _instance;
  private static final SimpleDateFormat _formatDate = new SimpleDateFormat("yyyy-MM-dd");
  private static final SimpleDateFormat _formatIsoDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

  private OSMTimeStampFormatter() {}

  public static OSMTimeStampFormatter getInstance() {
    if (_instance == null) _instance = new OSMTimeStampFormatter();
    return _instance;
  }

  public String date(Long tstamp) {
    return _formatDate.format(new Date(tstamp * 1000));
  }

  public String isoDateTime(Long tstamp) {
    return _formatIsoDateTime.format(new Date(tstamp * 1000));
  }
}
