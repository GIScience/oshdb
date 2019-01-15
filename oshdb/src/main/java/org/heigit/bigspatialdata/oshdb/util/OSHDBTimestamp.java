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

  public void setTimestamp(long tstamp) {
    this._tstamp = tstamp;
  }

  @Override
  public int compareTo(OSHDBTimestamp other) {
    return Long.compare(this._tstamp, other._tstamp);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (_tstamp ^ (_tstamp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    OSHDBTimestamp other = (OSHDBTimestamp) obj;
    if (_tstamp != other._tstamp)
      return false;
    return true;
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
