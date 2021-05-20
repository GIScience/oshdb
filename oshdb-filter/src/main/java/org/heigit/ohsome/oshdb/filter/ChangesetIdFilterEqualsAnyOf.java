package org.heigit.ohsome.oshdb.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.jetbrains.annotations.Contract;

/**
 * A filter which selects OSM contributions by matching to a list of changeset ids.
 */
public class ChangesetIdFilterEqualsAnyOf extends NegatableFilter {
  private final Collection<Long> changesetIdList;

  ChangesetIdFilterEqualsAnyOf(@Nonnull Collection<Long> changesetIdList) {
    super(new FilterInternal() {
      private final Set<Long> changesetIds = new HashSet<>(changesetIdList);

      @Override
      public boolean applyOSH(OSHEntity entity) {
        return applyToOSHEntityRecursively(entity, v -> changesetIds.contains(v.getChangesetId()));
      }

      @Override
      public boolean applyOSM(OSMEntity entity) {
        return true;
      }

      @Override
      public boolean applyOSMContribution(OSMContribution contribution) {
        return changesetIds.contains(contribution.getChangesetId());
      }

      @Override
      public boolean applyOSMEntitySnapshot(OSMEntitySnapshot ignored) {
        throw new IllegalStateException("changeset filter is not applicable to entity snapshots");
      }

      @Override
      public String toString() {
        return "changeset:in(" + changesetIdList.stream().map(String::valueOf)
            .collect(Collectors.joining(",")) + ")";
      }
    });
    this.changesetIdList = changesetIdList;
  }

  @Contract(pure = true)
  public Collection<Long> getChangesetIdList() {
    return this.changesetIdList;
  }
}
