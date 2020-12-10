package org.heigit.ohsome.oshdb.util.taginterpreter;

import java.io.Serializable;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMRelation;

/**
 * Used to provided information needed to create actual geometries from OSM data.
 *
 * <p>
 * Some information about OSM entities is only soft-coded into their tags, for example if a closed
 * way should represent a polygon (e.g. a building) or just a (linestring) loop (e.g. a roundabout).
 * Similarly, some information needed to build geometries from multipolygon relations is encoded
 * into the relation members' roles.
 * </p>
 */
public interface TagInterpreter extends Serializable {

  boolean isArea(OSMEntity entity);

  boolean isLine(OSMEntity entity);

  boolean hasInterestingTagKey(OSMEntity osm);

  boolean isMultipolygonOuterMember(OSMMember osmMember);

  boolean isMultipolygonInnerMember(OSMMember osmMember);

  boolean isOldStyleMultipolygon(OSMRelation osmRelation);
}
