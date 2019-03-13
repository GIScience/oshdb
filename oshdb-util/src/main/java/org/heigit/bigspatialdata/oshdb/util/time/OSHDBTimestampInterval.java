package org.heigit.bigspatialdata.oshdb.util.time;

import java.io.Serializable;
import java.util.SortedSet;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSHDBTimestampInterval implements Serializable, Comparable<OSHDBTimestampInterval> {
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

  public int compareTo(@Nonnull OSHDBTimestamp timestamp) {
    if (this.includes(timestamp)) {
      return 0;
    }
    return timestamp.getRawUnixTimestamp() < this.fromTimestamp.getRawUnixTimestamp() ? -1 : 1;
  }

  @Override
  public int compareTo(@Nonnull OSHDBTimestampInterval o) {
    return this.fromTimestamp.compareTo(o.fromTimestamp);
  }

  @Override
  public boolean equals(Object o) {
    return o != null && o instanceof OSHDBTimestampInterval &&
        this.fromTimestamp.equals(((OSHDBTimestampInterval) o).fromTimestamp) &&
        this.toTimestamp.equals(((OSHDBTimestampInterval) o).toTimestamp);
  }
}