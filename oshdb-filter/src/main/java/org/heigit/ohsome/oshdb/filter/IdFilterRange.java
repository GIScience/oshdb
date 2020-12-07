package org.heigit.ohsome.oshdb.filter;

import java.io.Serializable;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;

/**
 * A filter which executes a "id [not] in range" check.
 */
public class IdFilterRange extends NegatableFilter {
  static class IdRange implements Serializable {
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

  IdFilterRange(@Nonnull IdRange range) {
    super(new FilterInternal() {
      @Override
      public boolean applyOSH(OSHEntity entity) {
        return range.test(entity.getId());
      }

      @Override
      public boolean applyOSM(OSMEntity entity) {
        return range.test(entity.getId());
      }

      @Override
      public String toString() {
        return "id:in-range" + range.toString();
      }
    });
  }
}