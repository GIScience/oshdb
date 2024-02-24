package org.heigit.ohsome.oshdb.filter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.jetbrains.annotations.Contract;

/**
 * A filter which selects OSM contributions by matching to a list of changeset ids.
 */
public class ChangesetIdFilterEqualsAnyOf extends NegatableFilter {
  private final Set<Long> changesetIds;

  ChangesetIdFilterEqualsAnyOf(@Nonnull Collection<Long> changesetIdList) {
    this(new HashSet<>(changesetIdList));
  }
  ChangesetIdFilterEqualsAnyOf(@Nonnull Set<Long> changesetIds) {
    super(new FilterInternal() {

      @Override
      public boolean applyOSH(OSHEntity entity) {
        return applyToOSHEntityRecursively(entity, v -> changesetIds.contains(v.getChangesetId()));
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
        return "changeset:in(" + changesetIds.stream().map(String::valueOf)
            .collect(Collectors.joining(",")) + ")";
      }
    });
    this.changesetIds = changesetIds;
  }

  @Contract(pure = true)
  public Set<Long> getChangesetIdList() {
    return this.changesetIds;
  }
}
