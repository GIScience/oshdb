package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;

/**
 * A filter which selects OSM contributions by a range of contributor user ids.
 */
public class ContributorUserIdFilterRange extends NegatableFilter {
  ContributorUserIdFilterRange(IdRange contributorUserIdRange) {
    super(new FilterInternal() {
      @Override
      public boolean applyOSH(OSHEntity entity) {
        return applyToOSHEntityRecursively(entity, v -> contributorUserIdRange.test(v.getUserId()));
      }

      @Override
      public boolean applyOSMContribution(OSMContribution contribution) {
        return contributorUserIdRange.test(contribution.getContributorUserId());
      }

      @Override
      public boolean applyOSMEntitySnapshot(OSMEntitySnapshot ignored) {
        throw new IllegalStateException("contributor filter is not applicable to entity snapshots");
      }

      @Override
      public String toString() {
        return "contributor:in-range" + contributorUserIdRange;
      }
    });
  }
}
