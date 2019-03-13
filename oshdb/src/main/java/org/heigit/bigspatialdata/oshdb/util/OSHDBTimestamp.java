package org.heigit.bigspatialdata.oshdb.util;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class OSHDBTimestamp implements Comparable<OSHDBTimestamp>, Serializable {
  private static final long serialVersionUID = 1L;
  private long _tstamp;

  public OSHDBTimestamp(long tstamp) {
    this._tstamp = tstamp;
  }

  public OSHDBTimestamp(Date tstamp) {
    this(tstamp.getTime() / 1000);
  }
  
  public void setTimestamp(long tstamp){
    this._tstamp = tstamp;
  }

  @Override
  public int compareTo(OSHDBTimestamp other) {
    return Long.compare(this._tstamp, other._tstamp);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OSHDBTimestamp)
      return this._tstamp == ((OSHDBTimestamp)other)._tstamp;
    else
      return super.equals(other);
  }
  
  public Date toDate() {
    return new Date(this._tstamp * 1000);
  }

  public long getRawUnixTimestamp() {
    return this._tstamp;
  }

  public String toString() {
    ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(_tstamp), ZoneOffset.UTC);
    return zdt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  }
}
