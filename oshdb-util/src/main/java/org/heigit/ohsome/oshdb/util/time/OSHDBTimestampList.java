package org.heigit.ohsome.oshdb.util.time;

import java.io.Serializable;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;

/**
 * Provider of a sorted list of (unix) timestamps.
 */
public interface OSHDBTimestampList extends Serializable {
  /**
   * Provides a sorted set of OSHDBTimestamps.
   *
   * @return a sorted set of oshdb timestamps
   */
  TreeSet<OSHDBTimestamp> get();

  /**
   * Convenience method that converts the timestamp list into raw unix timestamps (long values).
   *
   * @return this list of timestamps as raw unix timestamps (measured in seconds)
   */
  default TreeSet<Long> getRawUnixTimestamps() {
    return this.get().stream()
        .map(OSHDBTimestamp::getEpochSecond)
        .collect(Collectors.toCollection(TreeSet::new));
  }
}
