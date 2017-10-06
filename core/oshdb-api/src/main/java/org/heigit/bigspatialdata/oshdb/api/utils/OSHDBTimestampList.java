package org.heigit.bigspatialdata.oshdb.api.utils;

import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Provider of a sorted list of (unix) timestamps.
 */
public interface OSHDBTimestampList {
  /**
   * Provides a sorted list of (unix) timestamps.
   *
   * @return a list of unix timestamps (measured in seconds)
   */
  List<Long> getTimestamps();

  /**
   * Convenience method that converts the timestamp list into OSHDBTimestamp objects,
   * which provide further convenience methods like toString formatting.
   *
   * @return this list of timestamps as OSHDBTimestamp objects
   */
  default List<OSHDBTimestamp> getOSHDBTimestamps() {
    return this.getTimestamps().stream().map(OSHDBTimestamp::new).collect(Collectors.toList());
  };
}
