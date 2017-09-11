package org.heigit.bigspatialdata.oshdb.osm;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import java.io.Serializable;
import java.util.*;
import javax.json.JsonObjectBuilder;

import org.heigit.bigspatialdata.oshdb.util.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

public class OSMNode extends OSMEntity implements Comparable<OSMNode>, Serializable {

  public static final double GEOM_PRECISION = .0000001; // osm only support 7 decimals

  private static final long serialVersionUID = 1L;

  private final long longitude;
  private final long latitude;

  public OSMNode(final long id, final int version, final long timestamp, final long changeset,
          final int userId, final int[] tags, final long longitude, final long latitude) {
    super(id, version, timestamp, changeset, userId, tags);
    this.longitude = longitude;
    this.latitude = latitude;
  }

  @Override
  public OSMType getType() {
    return OSMType.NODE;
  }

  public double getLongitude() {
    return longitude * GEOM_PRECISION;
  }

  public double getLatitude() {
    return latitude * GEOM_PRECISION;
  }

  public long getLon() {
    return longitude;
  }

  public long getLat() {
    return latitude;
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH, "NODE: %s %f:%f", super.toString(), getLongitude(), getLatitude());
  }

  @Override
  public String toString(TagTranslator tagTranslator) {
    return String.format(Locale.ENGLISH, "NODE: %s %f:%f", super.toString(tagTranslator), getLongitude(), getLatitude());
  }

  public boolean equalsTo(OSMNode o) {
    return super.equalsTo(o) && longitude == o.longitude && latitude == o.latitude;
  }

  @Override
  public int compareTo(OSMNode o) {
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
  }

  @Override
  public boolean isPoint() {
    return true;
    // ?? only if has tags and not: return !this.isAuxiliary();
  }

  @Override
  public boolean isPointLike(TagInterpreter areaDecider) {
    return this.isPoint();
  }

  @Override
  public boolean isArea(TagInterpreter areaDecider) {
    return false;
  }

  @Override
  public boolean isLine(TagInterpreter areaDecider) {
    return false;
  }

  @Override
  public Geometry getGeometry(long timestamp, TagInterpreter ti) {
    GeometryFactory geometryFactory = new GeometryFactory();
    return geometryFactory.createPoint(new Coordinate(this.getLongitude(), this.getLatitude()));
  }

  public Point getGeometry() {
    return (Point) this.getGeometry(-1, null);
  }

  @Override
  public String toGeoJSON(long timestamp, TagTranslator tagtranslator, TagInterpreter areaDecider) {
    String result = this.toGeoJSONbuilder(timestamp, tagtranslator, areaDecider).build().toString();
    return result;
  }

  @Override
  public JsonObjectBuilder toGeoJSONbuilder(long timestamp, TagTranslator tagtranslator, TagInterpreter areaDecider) {
    return super.toGeoJSONbuilder(timestamp, tagtranslator, areaDecider);
  }

}
