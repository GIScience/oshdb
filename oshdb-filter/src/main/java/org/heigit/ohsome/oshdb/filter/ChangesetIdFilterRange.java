package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;

/**
 * A filter which selects OSM contributions by matching to a range of changeset ids.
 */
public class ChangesetIdFilterRange extends NegatableFilter {
  private final IdRange changesetIdRange;

  ChangesetIdFilterRange(IdRange changesetIdRange) {
    super(new FilterInternal() {
      @Override
      public boolean applyOSH(OSHEntity entity) {
        return applyToOSHEntityRecursively(entity, v -> changesetIdRange.test(v.getChangesetId()));
      }

      @Override
      public boolean applyOSMContribution(OSMContribution contribution) {
        return changesetIdRange.test(contribution.getChangesetId());
      }

      @Override
      public boolean applyOSMEntitySnapshot(OSMEntitySnapshot ignored) {
        throw new IllegalStateException("changeset filter is not applicable to entity snapshots");
      }

      @Override
      public String toString() {
        return "changeset:in-range" + changesetIdRange;
      }
    });
    this.changesetIdRange = changesetIdRange;
  }

  public IdRange getChangesetIdRange() {
    return changesetIdRange;
  }
}
