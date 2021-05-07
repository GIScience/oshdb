package org.heigit.ohsome.oshdb.util.time;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedSet;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;

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
    return other.toTimestamp.getEpochSecond() >= this.fromTimestamp.getEpochSecond()
        && other.fromTimestamp.getEpochSecond() <= this.toTimestamp.getEpochSecond();
  }

  public boolean includes(OSHDBTimestamp timestamp) {
    return timestamp.getEpochSecond() >= this.fromTimestamp.getEpochSecond()
        && timestamp.getEpochSecond() < this.toTimestamp.getEpochSecond();
  }

  @SuppressWarnings("MissingJavadocMethod")
  public int compareAgainstTimestamp(@Nonnull OSHDBTimestamp timestamp) {
    if (this.includes(timestamp)) {
      return 0;
    }
    return timestamp.getEpochSecond() < this.fromTimestamp.getEpochSecond() ? -1 : 1;
  }

  @Override
  public int compareTo(@Nonnull OSHDBTimestampInterval o) {
    int c = this.fromTimestamp.compareTo(o.fromTimestamp);
    if (c == 0) {
      c = this.toTimestamp.compareTo(o.toTimestamp);
    }
    return c;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof OSHDBTimestampInterval
        && this.fromTimestamp.equals(((OSHDBTimestampInterval) o).fromTimestamp)
        && this.toTimestamp.equals(((OSHDBTimestampInterval) o).toTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromTimestamp, toTimestamp);
  }
}