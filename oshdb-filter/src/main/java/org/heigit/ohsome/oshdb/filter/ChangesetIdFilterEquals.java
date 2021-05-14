package org.heigit.ohsome.oshdb.filter;

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
    });
    this.changesetId = changesetId;
  }

  /**
   * Returns the OSM type of this filter.
   *
   * @return the OSM type of this filter.
   */
  @Contract(pure = true)
  public long getChangesetId() {
    return this.changesetId;
  }

  @Override
  public String toString() {
    return "changeset:" + this.changesetId;
  }
}
