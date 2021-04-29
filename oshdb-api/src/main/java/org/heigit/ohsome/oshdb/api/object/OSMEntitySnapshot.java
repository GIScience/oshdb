package org.heigit.ohsome.oshdb.api.object;

import com.google.common.collect.ComparisonChain;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateByTimestampEntry;
import org.heigit.ohsome.oshdb.util.celliterator.LazyEvaluatedObject;
import org.locationtech.jts.geom.Geometry;

/**
 * Stores information about a single data entity at a specific time "snapshot".
 *
 * <p>Alongside the entity and the timestamp, also the entity's geometry is provided.</p>
 */
public class OSMEntitySnapshot implements OSHDBMapReducible, Comparable<OSMEntitySnapshot> {
  private final IterateByTimestampEntry data;

  public OSMEntitySnapshot(IterateByTimestampEntry data) {
    this.data = data;
  }

  /**
   * Creates a copy of the given entity snapshot object with an updated geometry.
   */
  public OSMEntitySnapshot(OSMEntitySnapshot other, Geometry reclippedGeometry) {
    this(other, new LazyEvaluatedObject<>(reclippedGeometry));
  }

  /**
   * Creates a copy of the given entity snapshot object with an updated geometry.
   */
  public OSMEntitySnapshot(
      OSMEntitySnapshot other,
      LazyEvaluatedObject<Geometry> reclippedGeometry
  ) {
    this.data = new IterateByTimestampEntry(
        other.data.timestamp,
        other.data.osmEntity,
        other.data.oshEntity,
        reclippedGeometry,
        other.data.unclippedGeometry
    );
  }

  /**
   * The timestamp for which the snapshot of this data entity has been obtained.
   *
   * @return snapshot timestamp as an OSHDBTimestamp object
   */
  public OSHDBTimestamp getTimestamp() {
    return data.timestamp;
  }

  /**
   * The geometry of this entity at the snapshot's timestamp clipped to the requested area of
   * interest.
   *
   * @return the geometry as a JTS Geometry
   */
  public Geometry getGeometry() {
    return data.geometry.get();
  }

  /**
   * The geometry of this entity at the snapshot's timestamp. This is the full (unclipped) geometry
   * of the osm entity.
   *
   * @return the unclipped geometry of the osm entity snapshot as a JTS Geometry
   */
  public Geometry getGeometryUnclipped() {
    return data.unclippedGeometry.get();
  }

  /**
   * The entity for which the snapshot has been obtained.
   *
   * <p>This is the (not deleted) version of a OSHEntity that was valid at the provided snapshot
   * timestamp.</p>
   *
   * @return the OSMEntity object of this snapshot
   */
  public OSMEntity getEntity() {
    return data.osmEntity;
  }

  /**
   * The (parent) osh entity of the osm entity for which the snapshot has been obtained.
   *
   * @return the OSHEntity object corresponding to this snapshot
   */
  public OSHEntity getOSHEntity() {
    return data.oshEntity;
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
