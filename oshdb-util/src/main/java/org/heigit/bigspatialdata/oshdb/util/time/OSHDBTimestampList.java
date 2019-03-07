package org.heigit.bigspatialdata.oshdb.util.time;

import java.io.Serializable;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

/**
 * Provider of a sorted list of (unix) timestamps.
 */
public interface OSHDBTimestampList extends Serializable {
  /**
   * Provides a sorted set of OSHDBTimestamps.
   *
   * @return a sorted set of oshdb timestamps
   */
  SortedSet<OSHDBTimestamp> get();

  /**
   * Convenience method that converts the timestamp list into raw unix timestamps (long values)
   *
   * @return this list of timestamps as raw unix timestamps (measured in seconds)
   */
  default SortedSet<Long> getRawUnixTimestamps() {
    return this.get().stream()
        .map(OSHDBTimestamp::getRawUnixTimestamp)
        .collect(Collectors.toCollection(TreeSet::new));
  }
}
