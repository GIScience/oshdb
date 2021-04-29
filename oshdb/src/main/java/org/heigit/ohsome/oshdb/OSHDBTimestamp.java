package org.heigit.ohsome.oshdb;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class OSHDBTimestamp implements OSHDBTemporal, Comparable<OSHDBTimestamp>, Serializable {
  private static final long serialVersionUID = 1L;
  private final long epochSecond;

  public OSHDBTimestamp(long tstamp) {
    this.epochSecond = tstamp;
  }

  public OSHDBTimestamp(Date tstamp) {
    this(tstamp.getTime() / 1000);
  }
  
  @Override
  public int compareTo(OSHDBTimestamp other) {
    return Long.compare(this.epochSecond, other.epochSecond);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OSHDBTimestamp) {
      return this.epochSecond == ((OSHDBTimestamp) other).epochSecond;
    } else {
      return super.equals(other);
    }
  }

  @Override
  public int hashCode() {
    return Long.hashCode(this.epochSecond);
  }
  
  public Date toDate() {
    return new Date(this.epochSecond * 1000);
  }

  @Override
  public long getEpochSecond() {
    return this.epochSecond;
  }
  
  @Override
  public OSHDBTimestamp getTimestamp() {
    return this;
  }

  public String toString() {
    return OSHDBTemporal.toIsoDateTime(this);
  }
}
