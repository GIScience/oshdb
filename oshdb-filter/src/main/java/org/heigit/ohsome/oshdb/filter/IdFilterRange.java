package org.heigit.ohsome.oshdb.filter;

import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;

/**
 * A filter which executes a "id [not] in range" check.
 */
public class IdFilterRange extends NegatableFilter {
  IdFilterRange(@Nonnull IdRange range) {
    super(new FilterInternal() {
      @Override
      public boolean applyOSH(OSHEntity entity) {
        return range.test(entity.getId());
      }

      @Override
      boolean applyOSHNegated(OSHEntity entity) {
        return !this.applyOSH(entity);
      }

      @Override
      public boolean applyOSM(OSMEntity entity) {
        return range.test(entity.getId());
      }

      @Override
      boolean applyOSMNegated(OSMEntity entity) {
        return !this.applyOSM(entity);
      }

      @Override
      public String toString() {
        return "id:in-range" + range;
      }
    });
  }
}