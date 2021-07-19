package org.heigit.ohsome.oshdb.api.object;

import com.google.common.collect.ComparisonChain;
import java.util.EnumSet;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.heigit.ohsome.oshdb.util.celliterator.LazyEvaluatedContributionTypes;
import org.heigit.ohsome.oshdb.util.celliterator.LazyEvaluatedObject;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.locationtech.jts.geom.Geometry;

/**
 * Information about a single modification ("contribution") of a single OSM object.
 *
 * <p>
 * It holds the information about:
 * </p>
 * <ul>
 *   <li>the timestamp at which this change happened</li>
 *   <li>state of the entity before and after the modification</li>
 *   <li>the geometry of the entity before and after the modification</li>
 *   <li>the type(s) of change which has happened here (e.g. creation/deletion of an entity,
 *   modification of a geometry, altering of the tag list, etc.)</li>
 * </ul>
 */
public class OSMContributionImpl implements OSMContribution {
  private final IterateAllEntry data;

  public OSMContributionImpl(IterateAllEntry data) {
    this.data = data;
  }

  /**
   * Creates a copy of the given contribution object with an updated before/after geometry.
   */
  public OSMContributionImpl(
      OSMContribution other,
      Geometry reclippedGeometryBefore,
      Geometry reclippedGeometryAfter
  ) {
    this(other,
        new LazyEvaluatedObject<>(reclippedGeometryBefore),
        new LazyEvaluatedObject<>(reclippedGeometryAfter)
    );
  }

  /**
   * Creates a copy of the given contribution object with an updated before/after geometry.
   */
  public OSMContributionImpl(
      OSMContribution other,
      LazyEvaluatedObject<Geometry> reclippedGeometryBefore,
      LazyEvaluatedObject<Geometry> reclippedGeometryAfter
  ) {
    this.data = new IterateAllEntry(
        other.getTimestamp(),
        other.getEntityAfter(),
        other.getEntityBefore(),
        other.getOSHEntity(),
        reclippedGeometryAfter,
        reclippedGeometryBefore,
        new LazyEvaluatedObject<>(other::getGeometryUnclippedAfter),
        new LazyEvaluatedObject<>(other::getGeometryUnclippedBefore),
        new LazyEvaluatedContributionTypes(other::is),
        other.getChangesetId()
    );
  }

  @Override
  public OSHDBTimestamp getTimestamp() {
    return data.timestamp;
  }

  @Override
  public Geometry getGeometryBefore() {
    return data.previousGeometry.get();
  }

  @Override
  public Geometry getGeometryUnclippedBefore() {
    return data.unclippedPreviousGeometry.get();
  }

  @Override
  public Geometry getGeometryAfter() {
    return data.geometry.get();
  }

  @Override
  public Geometry getGeometryUnclippedAfter() {
    return data.unclippedGeometry.get();
  }

  @Override
  public OSMEntity getEntityBefore() {
    return data.previousOsmEntity;
  }

  @Override
  public OSMEntity getEntityAfter() {
    return data.osmEntity;
  }

  @Override
  public OSHEntity getOSHEntity() {
    return data.oshEntity;
  }

  @Override
  public boolean is(ContributionType contributionType) {
    return data.activities.contains(contributionType);
  }

  @Override
  public EnumSet<ContributionType> getContributionTypes() {
    return data.activities.get();
  }


  @Override
  public int getContributorUserId() {
    // todo: optimizable if done directly in CellIterator??
    OSMEntity entity = this.getEntityAfter();
    OSHDBTimestamp contributionTimestamp = this.getTimestamp();
    // if the entity itself was modified at this exact timestamp, or we know from the contribution
    // type that the entity must also have been modified, we can just return the uid directly
    if (contributionTimestamp.getEpochSecond() == entity.getEpochSecond()
        || this.getEntityBefore() == null
        || this.getEntityBefore().getVersion() != this.getEntityAfter().getVersion()
    ) {
      return entity.getUserId();
    }
    int userId = -1;
    // search children for actual contributor's userId
    if (entity instanceof OSMWay) {
      userId = ((OSMWay) entity).getMemberEntities(contributionTimestamp)
          .filter(Objects::nonNull)
          .filter(n -> n.getEpochSecond() == contributionTimestamp.getEpochSecond())
          .findFirst()
          .map(OSMEntity::getUserId)
          // "rare" race condition, caused by not properly ordered timestamps (t_x > t_{x+1})
          // todo: what to do here??
          .orElse(-1);
    } else if (entity instanceof OSMRelation) {
      userId = ((OSMRelation) entity).getMemberEntities(contributionTimestamp)
          .filter(Objects::nonNull)
          .filter(e -> e.getEpochSecond() == contributionTimestamp.getEpochSecond())
          .findFirst()
          .map(OSMEntity::getUserId)
          .orElseGet(() ->
              ((OSMRelation) entity).getMemberEntities(contributionTimestamp)
                  .filter(Objects::nonNull)
                  // todo: what to do with rel->node member changes or rel->rel[->*] changes?
                  .filter(e -> e instanceof OSMWay)
                  .map(e -> (OSMWay) e)
                  .flatMap(w -> w.getMemberEntities(contributionTimestamp))
                  .filter(Objects::nonNull)
                  .filter(n -> n.getEpochSecond() == contributionTimestamp.getEpochSecond())
                  .findFirst()
                  .map(OSMEntity::getUserId)
                  // possible "rare" race condition, caused by not properly ordered timestamps
                  // (t_x > t_{x+1}) // todo: what to do here??
                  .orElse(-1)
          );
    }
    return userId;
  }

  @Override
  public long getChangesetId() {
    return data.changeset;
  }

  @Override
  public int compareTo(@Nonnull OSMContribution other) {
    return ComparisonChain.start()
        .compare(this.getOSHEntity().getType(), other.getOSHEntity().getType())
        .compare(this.getOSHEntity().getId(), other.getOSHEntity().getId())
        .compare(this.getTimestamp(), other.getTimestamp())
        .result();
  }
}
