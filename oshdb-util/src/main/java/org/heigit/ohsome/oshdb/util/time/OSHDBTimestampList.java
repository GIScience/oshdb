package org.heigit.ohsome.oshdb.util.time;

import java.io.Serializable;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;

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

  default <X> SerializableFunction<X, OSHDBTimestamp>
      indexTimestamp(SerializableFunction<X, OSHDBTimestamp> indexer) {
    var timestamps = get();
    var minTime = timestamps.first();
    var maxTime = timestamps.last();
    return x -> {
      var ts = indexer.apply(x);
      if (ts == null || ts.compareTo(minTime) < 0 || ts.compareTo(maxTime) > 0) {
        throw new OSHDBInvalidTimestampException(
            "Aggregation timestamp outside of time query interval.");
      }
      return timestamps.floor(ts);
    };
  }
}
