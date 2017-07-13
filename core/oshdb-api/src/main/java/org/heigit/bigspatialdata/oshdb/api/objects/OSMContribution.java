package org.heigit.bigspatialdata.oshdb.api.objects;

import com.vividsolutions.jts.geom.Geometry;
import java.util.EnumSet;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.ContributionType;

public class OSMContribution {
  private final Timestamp _tstamp;
  private final Timestamp _validTo;
  private final Geometry _geometryBefore;
  private final Geometry _geometryAfter;
  private final OSMEntity _entityBefore;
  private final OSMEntity _entityAfter;
  private final EnumSet<ContributionType> _contributionTypes;
  
  public OSMContribution(Timestamp tstamp, Timestamp validTo, Geometry geometryBefore, Geometry geometryAfter, OSMEntity entityBefore, OSMEntity entityAfter, EnumSet<ContributionType> contributionTypes) {
    this._tstamp = tstamp;
    this._validTo = validTo;
    this._geometryBefore = geometryBefore;
    this._geometryAfter = geometryAfter;
    this._entityBefore = entityBefore;
    this._entityAfter = entityAfter;
    this._contributionTypes = contributionTypes;
  }
  
  public Timestamp getTimestamp() {
    return this._tstamp;
  }
  
  public Timestamp getValidTo() {
    return this._validTo;
  }
  
  public Geometry getGeometryBefore() {
    return this._geometryBefore;
  }
  
  public Geometry getGeometryAfter() {
    return this._geometryAfter;
  }
  
  public OSMEntity getEntityBefore() {
    return this._entityBefore;
  }
  
  public OSMEntity getEntityAfter() {
    return this._entityAfter;
  }
  
  public EnumSet<ContributionType> getContributionTypes() {
    return this._contributionTypes;
  }
}
