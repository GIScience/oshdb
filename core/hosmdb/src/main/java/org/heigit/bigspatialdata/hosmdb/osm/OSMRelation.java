package org.heigit.bigspatialdata.hosmdb.osm;

import com.vividsolutions.jts.geom.Geometry;
import org.heigit.bigspatialdata.hosmdb.util.tagInterpreter.TagInterpreter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Serializable;
import java.util.Set;


public class OSMRelation extends OSMEntity implements Comparable<OSMRelation>, Serializable {


  private static final long serialVersionUID = 1L;
  private final OSMMember[] members;

  public OSMRelation(final long id, final int version, final long timestamp, final long changeset,
      final int userId, final int[] tags, final OSMMember[] members) {
    super(id, version, timestamp, changeset, userId, tags);
    this.members = members;
  }


  public OSMMember[] getMembers() {
    return members;
  }

  @Override
  public int compareTo(OSMRelation o) {
    int c = Long.compare(id, o.id);
    if (c == 0) {
      c = Integer.compare(Math.abs(version), Math.abs(o.version));
      if (c == 0) {
        c = Long.compare(timestamp, o.timestamp);
      }
    }
    return c;
  }

  @Override
  public boolean isAuxiliary(Set<Integer> uninterestingTagKeys) {
    return false;
  }
  @Override
  public boolean isPoint() {
    return false;
  }
  @Override
  public boolean isPointLike(TagInterpreter areaDecider) {
    return this.isArea(areaDecider);
    // todo: also return true if relation type is site, restriction, etc.?
  }
  @Override
  public boolean isArea(TagInterpreter areaDecider) {
    return areaDecider.evaluateForArea(this);
  }
  @Override
  public boolean isLine(TagInterpreter areaDecider) {

    throw new NotImplementedException();
    // todo: return true if type=route in tags -> reuse areaDecider???
  }

  @Override
  public Geometry getGeometry(long timestamp, TagInterpreter areaDecider) {
    // bla

    throw new NotImplementedException();
  }

}
