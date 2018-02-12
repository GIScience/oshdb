package org.heigit.bigspatialdata.oshdb.api.object;

import com.vividsolutions.jts.geom.Geometry;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.celliterator.LazyEvaluatedObject;

/**
 * Stores information about a single data entity at a specific time "snapshot".
 *
 * Alongside the entity and the timestamp, also the entity's geometry is provided.
 */
public class OSMEntitySnapshot implements OSHDBMapReducible {
  private final OSHDBTimestamp _tstamp;
  private final LazyEvaluatedObject<Geometry> _geometry;
  private final OSMEntity _entity;
  
  public OSMEntitySnapshot(OSHDBTimestamp tstamp, LazyEvaluatedObject<Geometry> geometry, OSMEntity entity) {
    this._tstamp = tstamp;
    this._geometry = geometry;
    this._entity = entity;
  }

  /**
   * The timestamp for which the snapshot of this data entity has been obtained.
   *
   * @return snapshot timestamp as an OSHDBTimestamp object
   */
  public OSHDBTimestamp getTimestamp() {
    return this._tstamp;
  }

  /**
   * The geometry of this entity at the snapshot's timestamp.
   *
   * @return the geometry as a JTS Geometry
   */
  public Geometry getGeometry() {
    return this._geometry.get();
  }

  /**
   * The entity for which the snapshot has been obtained.
   *
   * This is the (not deleted) version of a OSHEntity that was valid at the provided snapshot timestamp.
   *
   * @return the OSMEntity object of this snapshot
   */
  public OSMEntity getEntity() {
    return this._entity;
  }
}
