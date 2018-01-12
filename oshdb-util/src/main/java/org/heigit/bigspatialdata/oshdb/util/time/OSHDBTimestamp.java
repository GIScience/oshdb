package org.heigit.bigspatialdata.oshdb.util.time;

import org.heigit.bigspatialdata.oshdb.util.time.TimestampFormatter;

import java.io.Serializable;
import java.util.Date;

public class OSHDBTimestamp implements Comparable<OSHDBTimestamp>, Serializable {
  private long _tstamp;
  private static final TimestampFormatter _timeStampFormatter = TimestampFormatter.getInstance();

  public OSHDBTimestamp(long tstamp) {
    this._tstamp = tstamp;
  }

  @Override
  public int compareTo(OSHDBTimestamp other) {
    return Long.compare(this._tstamp, other._tstamp);
  }
  
  public Date toDate() {
    return new Date(this._tstamp * 1000);
  }

  public long toLong() {
    return this._tstamp;
  }
  
  public String formatDate() {
    return _timeStampFormatter.date(this.toDate());
  }

  public String formatIsoDateTime() {
    return _timeStampFormatter.isoDateTime(this.toDate());
  }

  public String toString() {
    return this.formatIsoDateTime();
  }
}
