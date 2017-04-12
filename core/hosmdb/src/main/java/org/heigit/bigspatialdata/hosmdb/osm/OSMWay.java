package org.heigit.bigspatialdata.hosmdb.osm;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMNode;
import org.heigit.bigspatialdata.hosmdb.util.tagInterpreter.TagInterpreter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;


public class OSMWay extends OSMEntity implements Comparable<OSMWay>, Serializable {

  private static final long serialVersionUID = 1L;
  private final OSMMember[] refs;

  public OSMWay(final long id, final int version, final long timestamp, final long changeset,
      final int userId, final int[] tags, final OSMMember[] refs) {
    super(id, version, timestamp, changeset, userId, tags);
    this.refs = refs;
  }


  public OSMMember[] getRefs() {
    return refs;
  }
  
  @Override
  public String toString() {
    return String.format("WAY-> %s Refs:%s", super.toString(), Arrays.toString(getRefs()));
  }


  @Override
  public int compareTo(OSMWay o) {
    int c = Long.compare(id, o.id);
    if (c == 0)
      c = Integer.compare(Math.abs(version), Math.abs(o.version));
    if (c == 0)
      c = Long.compare(timestamp, o.timestamp);
    return c;
  }


  @Override
  public boolean isAuxiliary(Set<Integer> uninterestingTagKeys) {
    throw new NotImplementedException();
    // todo: return true if no own (except uninteresting) tags and member of a relation (e.g. multipolygon)
  }
  @Override
  public boolean isPoint() {
    return false;
  }
  @Override
  public boolean isPointLike(TagInterpreter areaDecider) {
    return this.isArea(areaDecider);
  }
  @Override
  public boolean isArea(TagInterpreter areaDecider) {
    OSMMember[] nds = this.getRefs();
    if (nds[0].getId() != nds[nds.length-1].getId())
      return false;
    return areaDecider.evaluateForArea(this);
  }
  @Override
  public boolean isLine(TagInterpreter areaDecider) {
    return areaDecider.evaluateForLine(this);
  }

  @Override
  public Geometry getGeometry(long timestamp, TagInterpreter areaDecider) {
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = Arrays.stream(this.getRefs())
    .map(d -> (HOSMNode)d.getData())
    .map(hosm -> hosm.getByTimestamp(timestamp))
    .map(osm -> (OSMNode)osm)
    .filter(node -> node != null && node.isVisible())
    .map(nd -> new Coordinate(nd.getLongitude(), nd.getLatitude()))
    .toArray(Coordinate[]::new);
    if (this.isLine(areaDecider)) {
      if (coords.length < 2)
        return null;
      return geometryFactory.createLineString(coords);
    } else {
      if (coords.length < 4)
        return null;
      return geometryFactory.createPolygon(coords);
    }
  }
}
