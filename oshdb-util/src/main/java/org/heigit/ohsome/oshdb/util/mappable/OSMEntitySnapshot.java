package org.heigit.ohsome.oshdb.util.mappable;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.locationtech.jts.geom.Geometry;

/**
 * Information about a single OSM object at a specific point in time ("snapshot").
 */
public interface OSMEntitySnapshot extends OSHDBMapReducible, Comparable<OSMEntitySnapshot> {
  /**
   * The timestamp for which the snapshot of this data entity has been obtained.
   *
   * @return snapshot timestamp as an OSHDBTimestamp object
   */
  OSHDBTimestamp getTimestamp();

  /**
   * The timestamp when the entity of the snapshot was last modified before the snapshot timestamp.
   *
   * @return last modification timestamp as an OSHDBTimestamp object
   */
  OSHDBTimestamp getLastContributionTimestamp();

  /**
   * The geometry of this entity at the snapshot's timestamp clipped to the requested area of
   * interest.
   *
   * @return the geometry as a JTS Geometry
   */
  Geometry getGeometry();

  /**
   * The geometry of this entity at the snapshot's timestamp. This is the full (unclipped) geometry
   * of the osm entity.
   *
   * @return the unclipped geometry of the osm entity snapshot as a JTS Geometry
   */
  Geometry getGeometryUnclipped();

  /**
   * The entity for which the snapshot has been obtained.
   *
   * <p>This is the (not deleted) version of a OSHEntity that was valid at the provided snapshot
   * timestamp.</p>
   *
   * @return the OSMEntity object of this snapshot
   */
  OSMEntity getEntity();

  /**
   * The (parent) osh entity of the osm entity for which the snapshot has been obtained.
   *
   * @return the OSHEntity object corresponding to this snapshot
   */
  OSHEntity getOSHEntity();
}
