package org.heigit.ohsome.oshdb.filter;

import java.io.Serializable;
import java.util.Objects;
import java.util.stream.LongStream;

/**
 * Helper class to handle ranges of ids (incl. user ids, changeset ids, etc.).
 *
 * <p>The range's limits are tested inclusively: the range (10..12) would match the values 10,
 * 11 and 12, but not 9 or 13 for example.</p>
 */
public class IdRange implements Serializable {

  private final long fromId;
  private final long toId;

  /**
   * Creates a new id range.
   *
   * @param fromId lower limit of the range.
   * @param toId upper limit of the range.
   */
  public IdRange(long fromId, long toId) {
    this.fromId = Math.min(fromId, toId);
    this.toId = Math.max(fromId, toId);
  }

  public IdRange(long id) {
    this(id, id);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof IdRange idRange)) return false;
    return fromId == idRange.fromId && toId == idRange.toId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromId, toId);
  }

  public long getFromId() {
    return fromId;
  }

  public long getToId() {
    return toId;
  }

  public long size() {
    return 1 + toId - fromId;
  }

  public LongStream getIds() {
    return LongStream.range(fromId, toId+1);
  }
}
