package org.heigit.ohsome.oshdb.util.mappable;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;

/**
 * Marks a class as possible data type of an OSHDB-MapReducer.
 */
public interface OSHDBMapReducible {
  /**
   * Returns the timestamp at which this data modification has happened.
   *
   * @return the modification timestamp as a OSHDBTimestamp object
   */
  OSHDBTimestamp getTimestamp();
}



