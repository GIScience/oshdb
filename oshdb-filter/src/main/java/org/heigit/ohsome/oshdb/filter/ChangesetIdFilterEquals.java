package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.jetbrains.annotations.Contract;

/**
 * A filter which selects OSM contributions by a changeset id.
 */
public class ChangesetIdFilterEquals extends NegatableFilter {
  final long changesetId;

  ChangesetIdFilterEquals(long changesetId) {
    super(new FilterInternal() {
      @Override
      public boolean applyOSH(OSHEntity entity) {
        return applyToOSHEntityRecursively(entity, v -> v.getChangesetId() == changesetId);
      }

      @Override
      public boolean applyOSM(OSMEntity entity) {
        return true;
      }

      @Override
      public boolean applyOSMContribution(OSMContribution contribution) {
        return contribution.getChangesetId() == changesetId;
      }

      @Override
      public boolean applyOSMEntitySnapshot(OSMEntitySnapshot ignored) {
        throw new IllegalStateException("changeset filter is not applicable to entity snapshots");
      }

      @Override
      public String toString() {
        return "changeset:" + changesetId;
      }
    });
    this.changesetId = changesetId;
  }

  @Contract(pure = true)
  public long getChangesetId() {
    return this.changesetId;
  }
}
