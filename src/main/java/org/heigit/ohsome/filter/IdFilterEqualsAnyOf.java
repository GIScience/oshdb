package org.heigit.ohsome.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

/**
 * A tag filter which executes a "id [not] in (id1, id2, …)" check.
 */
public class IdFilterEqualsAnyOf implements Filter {
  private final Set<Long> ids;
  private final boolean negated;

  IdFilterEqualsAnyOf(@Nonnull Collection<Long> ids) {
    this(ids, false);
  }

  private IdFilterEqualsAnyOf(@Nonnull Collection<Long> ids, boolean negated) {
    if (ids.isEmpty()) {
      throw new IllegalStateException("list of ids must not be empty in a id in (list) filter");
    }
    this.ids = new HashSet<>(ids);
    this.negated = negated;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return this.ids.contains(entity.getId()) ^ this.negated;
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return this.ids.contains(entity.getId()) ^ this.negated;
  }

  @Override
  public FilterExpression negate() {
    return new IdFilterEqualsAnyOf(this.ids, !this.negated);
  }

  @Override
  public String toString() {
    return (this.negated ? "id:not-in" : "id:in")
        + this.ids.stream().map(String::valueOf).collect(Collectors.joining(","));
  }
}
