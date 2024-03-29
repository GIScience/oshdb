package org.heigit.ohsome.oshdb.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;

/**
 * A tag filter which executes a "id [not] in (id1, id2, …)" check.
 */
public class IdFilterEqualsAnyOf extends NegatableFilter {
  IdFilterEqualsAnyOf(@Nonnull Collection<Long> idList) {
    super(new FilterInternal() {
      private final Set<Long> ids = new HashSet<>(idList);

      @Override
      public boolean applyOSH(OSHEntity entity) {
        return this.ids.contains(entity.getId());
      }

      @Override
      boolean applyOSHNegated(OSHEntity entity) {
        return !this.applyOSH(entity);
      }

      @Override
      public boolean applyOSM(OSMEntity entity) {
        return this.ids.contains(entity.getId());
      }

      @Override
      boolean applyOSMNegated(OSMEntity entity) {
        return !this.applyOSM(entity);
      }

      @Override
      public String toString() {
        return "id:in" + this.ids.stream().map(String::valueOf).collect(Collectors.joining(","));
      }
    });

    if (idList.isEmpty()) {
      throw new IllegalStateException("list of ids must not be empty in a id in (list) filter");
    }
  }
}