package org.heigit.ohsome.oshdb.filter;

import java.io.Serializable;

/**
 * Helper class to handle ranges of ids (incl. user ids, changeset ids, etc.).
 *
 * <p>The range's limits are tested inclusively: the range (10..12) would match the values 10,
 * 11 and 12, but not 9 or 13 for example.</p>
 */
class IdRange implements Serializable {

  private final long fromId;
  private final long toId;

  /**
   * Creates a new id range.
   *
   * @param fromId lower limit of the range.
   * @param toId upper limit of the range.
   */
  IdRange(long fromId, long toId) {
    if (toId < fromId) {
      long buffer = toId;
      toId = fromId;
      fromId = buffer;
    }
    this.fromId = fromId;
    this.toId = toId;
  }

  /** Checks if the given id falls into the id range. */
  public boolean test(long id) {
    return id >= fromId && id <= toId;
  }

  public String toString() {
    return (fromId == Long.MIN_VALUE ? "" : fromId)
        + ".."
        + (toId == Long.MAX_VALUE ? "" : toId);
  }
}
