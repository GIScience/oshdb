package org.heigit.bigspatialdata.oshdb.api.objects;

import com.vividsolutions.jts.geom.Geometry;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

public class OSMEntitySnapshot {
  private final OSHDBTimestamp _tstamp;
  private final Geometry _geometry;
  private final OSMEntity _entity;
  
  public OSMEntitySnapshot(OSHDBTimestamp tstamp, Geometry geometry, OSMEntity entity) {
    this._tstamp = tstamp;
    this._geometry = geometry;
    this._entity = entity;
  }
  
  public OSHDBTimestamp getTimestamp() {
    return this._tstamp;
  }
  
  public Geometry getGeometry() {
    return this._geometry;
  }
  
  public OSMEntity getEntity() {
    return this._entity;
  }
}
