package org.heigit.ohsome.oshdb.api.object;

import com.google.common.collect.ComparisonChain;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateByTimestampEntry;
import org.heigit.ohsome.oshdb.util.celliterator.LazyEvaluatedObject;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.locationtech.jts.geom.Geometry;

/**
 * Information about a single OSM object at a specific point in time ("snapshot").
 *
 * <p>Alongside the entity and the timestamp, also the entity's geometry is provided.</p>
 */
public class OSMEntitySnapshotImpl implements OSMEntitySnapshot {
  private final IterateByTimestampEntry data;

  public OSMEntitySnapshotImpl(IterateByTimestampEntry data) {
    this.data = data;
  }

  /**
   * Creates a copy of the given entity snapshot object with an updated geometry.
   */
  public OSMEntitySnapshotImpl(OSMEntitySnapshot other, Geometry reclippedGeometry) {
    this(other, new LazyEvaluatedObject<>(reclippedGeometry));
  }

  /**
   * Creates a copy of the given entity snapshot object with an updated geometry.
   */
  public OSMEntitySnapshotImpl(
      OSMEntitySnapshot other,
      LazyEvaluatedObject<Geometry> reclippedGeometry
  ) {
    this.data = new IterateByTimestampEntry(
        other.getTimestamp(),
        other.getLastContributionTimestamp(),
        other.getEntity(),
        other.getOSHEntity(),
        reclippedGeometry,
        new LazyEvaluatedObject<>(other::getGeometryUnclipped)
    );
  }

  @Override
  public OSHDBTimestamp getTimestamp() {
    return data.timestamp();
  }

  @Override
  public OSHDBTimestamp getLastContributionTimestamp() {
    return data.lastModificationTimestamp();
  }

  @Override
  public Geometry getGeometry() {
    return data.geometry().get();
  }

  @Override
  public Geometry getGeometryUnclipped() {
    return data.unclippedGeometry().get();
  }

  @Override
  public OSMEntity getEntity() {
    return data.osmEntity();
  }

  @Override
  public OSHEntity getOSHEntity() {
    return data.oshEntity();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Note: this class has a natural ordering that is inconsistent with equals.</p>
   */
  @Override
  public int compareTo(@Nonnull OSMEntitySnapshot other) {
    return ComparisonChain.start()
        .compare(this.getOSHEntity().getType(), other.getOSHEntity().getType())
        .compare(this.getOSHEntity().getId(), other.getOSHEntity().getId())
        .compare(this.getTimestamp(), other.getTimestamp())
        .result();
  }
}
