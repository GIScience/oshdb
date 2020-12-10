package org.heigit.ohsome.oshdb.util;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class OSHDBTimestamp implements Comparable<OSHDBTimestamp>, Serializable {
  private static final long serialVersionUID = 1L;
  private long tstamp;

  public OSHDBTimestamp(long tstamp) {
    this.tstamp = tstamp;
  }

  public OSHDBTimestamp(Date tstamp) {
    this(tstamp.getTime() / 1000);
  }
  
  public void setTimestamp(long tstamp){
    this.tstamp = tstamp;
  }

  @Override
  public int compareTo(OSHDBTimestamp other) {
    return Long.compare(this.tstamp, other.tstamp);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OSHDBTimestamp)
      return this.tstamp == ((OSHDBTimestamp)other).tstamp;
    else
      return super.equals(other);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(this.tstamp);
  }
  
  public Date toDate() {
    return new Date(this.tstamp * 1000);
  }

  public long getRawUnixTimestamp() {
    return this.tstamp;
  }

  public String toString() {
    ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(tstamp), ZoneOffset.UTC);
    return zdt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
  }
}
