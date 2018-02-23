package org.heigit.bigspatialdata.oshdb.util.time;

import java.io.Serializable;
import java.util.SortedSet;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSHDBTimestampInterval implements Serializable {
  private final OSHDBTimestamp fromTimestamp;
  private final OSHDBTimestamp toTimestamp;

  public OSHDBTimestampInterval() {
    this(new OSHDBTimestamp(Long.MIN_VALUE), new OSHDBTimestamp(Long.MAX_VALUE));
  }

  public OSHDBTimestampInterval(OSHDBTimestamp fromTimestamp, OSHDBTimestamp toTimestamp) {
    this.fromTimestamp = fromTimestamp;
    this.toTimestamp = toTimestamp;
  }

  public OSHDBTimestampInterval(SortedSet<OSHDBTimestamp> oshdbTimestamps) {
    this(oshdbTimestamps.first(), oshdbTimestamps.last());
  }

  public boolean intersects(OSHDBTimestampInterval other) {
    return other.toTimestamp.getRawUnixTimestamp() >= this.fromTimestamp.getRawUnixTimestamp()
        && other.fromTimestamp.getRawUnixTimestamp() <= this.toTimestamp.getRawUnixTimestamp();
  }

  public boolean includes(OSHDBTimestamp timestamp) {
    return timestamp.getRawUnixTimestamp() >= this.fromTimestamp.getRawUnixTimestamp()
        && timestamp.getRawUnixTimestamp() < this.toTimestamp.getRawUnixTimestamp();
  }

  public int compareTo(OSHDBTimestamp timestamp) {
    if (this.includes(timestamp)) {
      return 0;
    }
    return timestamp.getRawUnixTimestamp() < this.fromTimestamp.getRawUnixTimestamp() ? -1 : 1;
  }
}
