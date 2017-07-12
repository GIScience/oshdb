package org.heigit.bigspatialdata.oshdb.api.objects;

import java.util.Date;

public class Timestamp implements Comparable<Timestamp> {
  private Long _tstamp;
  private static final TimestampFormatter _timeStampFormatter = TimestampFormatter.getInstance();

  public Timestamp(Long tstamp) {
    this._tstamp = tstamp;
  }

  @Override
  public int compareTo(Timestamp other) {
    return Long.compare(this._tstamp, other._tstamp);
  }
  
  public Date getDate() {
    return new Date(this._tstamp * 1000);
  }
  
  public String date() {
    return this._timeStampFormatter.date(this.getDate());
  }

  public String isoDateTime() {
    return this._timeStampFormatter.isoDateTime(this.getDate());
  }
}
