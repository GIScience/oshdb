package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;

/**
 * A filter which executes a "id [not] in range" check.
 */
public class IdFilterRange extends NegatableFilter {
  IdFilterRange(@Nonnull IdRange range) {
    super(new FilterExpression() {
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
        return "id:in-range" + range;
      }
    });
  }
}