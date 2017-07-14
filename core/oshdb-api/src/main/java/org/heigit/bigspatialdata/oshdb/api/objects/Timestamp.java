package org.heigit.bigspatialdata.oshdb.api.objects;

import org.heigit.bigspatialdata.oshdb.api.utils.TimestampFormatter;
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
  
  public Date toDate() {
    return new Date(this._tstamp * 1000);
  }
  
  public String formatDate() {
    return this._timeStampFormatter.date(this.toDate());
  }

  public String formatIsoDateTime() {
    return this._timeStampFormatter.isoDateTime(this.toDate());
  }
}
