package org.heigit.bigspatialdata.oshdb.utils;

public class OSMTimeStamp implements Comparable<OSMTimeStamp> {
  private Long _tstamp;
  private static final OSMTimeStampFormatter _timeStampFormatter = OSMTimeStampFormatter.getInstance();

  public OSMTimeStamp(Long tstamp) {
    this._tstamp = tstamp;
  }

  @Override
  public int compareTo(OSMTimeStamp other) {
    return Long.compare(this._tstamp, other._tstamp);
  }

  public String date() {
    return this._timeStampFormatter.date(this._tstamp);
  }

  public String isoDateTime() {
    return this._timeStampFormatter.isoDateTime(this._tstamp);
  }
}
