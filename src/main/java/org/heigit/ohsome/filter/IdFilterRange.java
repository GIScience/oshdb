package org.heigit.ohsome.filter;

import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

/**
 * A tag filter which executes a "id [not] in range" check.
 */
public class IdFilterRange implements Filter {
  static class IdRange {
    private final long fromId;
    private final long toId;

    IdRange(long fromId, long toId) {
      if (toId < fromId) {
        long buffer = toId;
        toId = fromId;
        fromId = buffer;
      }
      this.fromId = fromId;
      this.toId = toId;
    }

    private boolean test(long id) {
      return id >= fromId && id <= toId;
    }

    public String toString() {
      return (fromId == Long.MIN_VALUE ? "" : fromId)
          + ".."
          + (toId == Long.MAX_VALUE ? "" : toId);
    }
  }

  private final IdRange idRange;
  private final boolean negated;

  IdFilterRange(@Nonnull IdRange idRange) {
    this(idRange, false);
  }

  private IdFilterRange(@Nonnull IdRange idRange, boolean negated) {
    this.idRange = idRange;
    this.negated = negated;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return this.idRange.test(entity.getId()) ^ this.negated;
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return this.idRange.test(entity.getId()) ^ this.negated;
  }

  @Override
  public FilterExpression negate() {
    return new IdFilterRange(this.idRange, !this.negated);
  }

  @Override
  public String toString() {
    return (this.negated ? "id:not-in-range" : "id:in-range") + idRange.toString();
  }
}
