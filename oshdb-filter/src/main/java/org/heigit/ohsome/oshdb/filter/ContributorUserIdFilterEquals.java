package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.jetbrains.annotations.Contract;

/**
 * A filter which selects OSM contributions by a user id.
 */
public class ContributorUserIdFilterEquals extends NegatableFilter {
  final long userId;

  ContributorUserIdFilterEquals(long userId) {
    super(new FilterInternal() {
      @Override
      public boolean applyOSM(OSMEntity entity) {
        return true;
      }

      @Override
      public boolean applyOSMContribution(OSMContribution contribution) {
        return contribution.getContributorUserId() == userId;
      }

      @Override
      public boolean applyOSMEntitySnapshot(OSMEntitySnapshot ignored) {
        throw new IllegalStateException("contributor filter is not applicable to entity snapshots");
      }
    });
    this.userId = userId;
  }

  @Contract(pure = true)
  public long getUserId() {
    return this.userId;
  }

  @Override
  public String toString() {
    return "contributor:" + this.userId;
  }
}
