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
 * A filter which selects OSM contributions by matching to a list of contributor user ids.
 */
public class ContributorUserIdFilterEqualsAnyOf extends NegatableFilter {
  private final Collection<Integer> contributorUserIdList;

  ContributorUserIdFilterEqualsAnyOf(@Nonnull Collection<Integer> contributorUserIdList) {
    super(new FilterInternal() {
      private final Set<Integer> contributorUserIds = new HashSet<>(contributorUserIdList);

      @Override
      public boolean applyOSH(OSHEntity entity) {
        return applyToOSHEntityRecursively(entity, v -> contributorUserIds.contains(v.getUserId()));
      }

      @Override
      public boolean applyOSMContribution(OSMContribution contribution) {
        return contributorUserIds.contains(contribution.getContributorUserId());
      }

      @Override
      public boolean applyOSMEntitySnapshot(OSMEntitySnapshot ignored) {
        throw new IllegalStateException("contributor filter is not applicable to entity snapshots");
      }

      @Override
      public String toString() {
        return "contributor:in(" + contributorUserIdList.stream().map(String::valueOf)
            .collect(Collectors.joining(",")) + ")";
      }
    });
    this.contributorUserIdList = contributorUserIdList;
  }

  @Contract(pure = true)
  public Collection<Integer> getContributorUserIdList() {
    return this.contributorUserIdList;
  }
}
