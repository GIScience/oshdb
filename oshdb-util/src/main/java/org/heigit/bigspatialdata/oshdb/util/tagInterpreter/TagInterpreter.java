package org.heigit.bigspatialdata.oshdb.util.tagInterpreter;

import java.io.Serializable;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;

/**
 * Used to provided information needed to create actual geometries from OSM data:
 *
 * Some information about OSM entities is only soft-coded into their tags, for example if a closed
 * way should represent a polygon (e.g. a building) or just a (linestring) loop (e.g. a roundabout).
 * Similarly, some information needed to build geometries from multipolygon relations is encoded
 * into the relation members' roles.
 */
public interface TagInterpreter extends Serializable {

  boolean isArea(OSMEntity entity);

  boolean isLine(OSMEntity entity);

  boolean hasInterestingTagKey(OSMEntity osm);

  boolean isMultipolygonOuterMember(OSMMember osmMember);

  boolean isMultipolygonInnerMember(OSMMember osmMember);

  boolean isOldStyleMultipolygon(OSMRelation osmRelation);
}
