package org.heigit.bigspatialdata.oshdb.osm;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.util.OSMType;
import org.heigit.bigspatialdata.oshdb.util.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

public class OSMWay extends OSMEntity implements Comparable<OSMWay>, Serializable {

  private static final long serialVersionUID = 1L;
  private final OSMMember[] refs;

  public OSMWay(final long id, final int version, final long timestamp, final long changeset,
          final int userId, final int[] tags, final OSMMember[] refs) {
    super(id, version, timestamp, changeset, userId, tags);
    this.refs = refs;
  }

  @Override
  public OSMType getType() {
    return OSMType.WAY;
  }

  public OSMMember[] getRefs() {
    return refs;
  }

  public Stream<OSMNode> getRefEntities(long timestamp) {
    return Arrays.stream(this.getRefs())
            .map(OSMMember::getEntity)
            .filter(Objects::nonNull)
            .map(entity -> ((OSHNode) entity).getByTimestamp(timestamp));
  }

  @Override
  public String toString() {
    return String.format("WAY-> %s Refs:%s", super.toString(), Arrays.toString(getRefs()));
  }

  @Override
  public String toString(TagTranslator tagTranslator) {
    StringBuilder sb = new StringBuilder();
    sb.append("WAY-> ").append(super.toString(tagTranslator)).append(" Refs:").append("[");
    for (int i = 0; i < getRefs().length; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(getRefs()[i].getId());
    }
    sb.append("]");

    return sb.toString();
  }

  @Override
  public int compareTo(OSMWay o) {
    int c = Long.compare(id, o.id);
    if (c == 0) {
      c = Integer.compare(Math.abs(version), Math.abs(o.version));
    }
    if (c == 0) {
      c = Long.compare(timestamp, o.timestamp);
    }
    return c;
  }

  @Override
  public boolean isAuxiliary(Set<Integer> uninterestingTagKeys) {
    throw new UnsupportedOperationException("Not supported yet.");
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
    if (nds.length < 4 || nds[0].getId() != nds[nds.length - 1].getId()) {
      return false;
    }
    return areaDecider.evaluateForArea(this);
  }

  @Override
  public boolean isLine(TagInterpreter areaDecider) {
    return !this.isArea(areaDecider);
  }

  @Override
  public Geometry getGeometry(long timestamp, TagInterpreter areaDecider) {
    // todo: handle old-style multipolygons here???
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = this.getRefEntities(timestamp)
            .filter(node -> node != null && node.isVisible())
            .map(nd -> new Coordinate(nd.getLongitude(), nd.getLatitude()))
            .toArray(Coordinate[]::new);
    if (this.isLine(areaDecider)) {
      if (coords.length < 2) {
        return null; // better: "invalid line geometry" exception?
      }
      return geometryFactory.createLineString(coords);
    } else {
      if (coords.length < 4) {
        return null; // better: "invalid polygon geometry" exception?
      }
      return geometryFactory.createPolygon(coords);
    }
  }

  /**
   * Get a GIS-compatible String version of your OSM-Object.
   *
   * @param timestamp The timestamp for which to create the geometry. NB: the
   * geometry will be created for exactly that point in time (see
   * this.getGeometry()).
   * @param tagtranslator a connection to a database to translate the coded
   * integer back to human readable string
   * @param areaDecider A list of tags, that define a polygon from a linestring.
   * A default one is available.
   * @return A string representation of the Object in GeoJSON-format
   * (https://tools.ietf.org/html/rfc7946#section-3.3)
   */
  public String toGeoJSON(long timestamp, TagTranslator tagtranslator, TagInterpreter areaDecider) {
    JsonArrayBuilder nd = Json.createArrayBuilder();
    for (OSMMember node : getRefs()) {
      nd.add(node.getId());
    }
    String result = this.toGeoJSONbuilder(timestamp, tagtranslator, areaDecider).add("refs", nd).build().toString();
    return result;
  }

}
