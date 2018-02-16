package org.heigit.bigspatialdata.oshdb.util.tagInterpreter;

import java.io.Serializable;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;

/**
 * methods used to determine whether a OSM way represents a polygon or linestring geometry.
 */
public interface TagInterpreter extends Serializable {

  boolean isArea(OSMEntity entity);

  boolean isLine(OSMEntity entity);

  boolean hasInterestingTagKey(OSMEntity osm);

  boolean isMultipolygonOuterMember(OSMMember osmMember);

  boolean isMultipolygonInnerMember(OSMMember osmMember);

  boolean isOldStyleMultipolygon(OSMRelation osmRelation);
}
