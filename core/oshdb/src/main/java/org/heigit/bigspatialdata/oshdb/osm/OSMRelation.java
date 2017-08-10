package org.heigit.bigspatialdata.oshdb.osm;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import org.heigit.bigspatialdata.oshdb.util.OSMType;
import org.heigit.bigspatialdata.oshdb.util.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

public class OSMRelation extends OSMEntity implements Comparable<OSMRelation>, Serializable {

  private static final long serialVersionUID = 1L;
  private final OSMMember[] members;

  public OSMRelation(final long id, final int version, final long timestamp, final long changeset,
          final int userId, final int[] tags, final OSMMember[] members) {
    super(id, version, timestamp, changeset, userId, tags);
    this.members = members;
  }

  @Override
  public OSMType getType() {
    return OSMType.RELATION;
  }

  public OSMMember[] getMembers() {
    return members;
  }

  public Stream<OSMEntity> getMemberEntities(long timestamp, Predicate<OSMMember> memberFilter) {
    return Arrays.stream(this.getMembers())
            .filter(memberFilter)
            .map(OSMMember::getEntity)
            .filter(Objects::nonNull)
            .map(entity -> entity.getByTimestamp(timestamp));
  }

  public Stream<OSMEntity> getMemberEntities(long timestamp) {
    return this.getMemberEntities(timestamp, osmMember -> true);
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
    return areaDecider.evaluateForLine(this);
  }

  @Override
  public Geometry getGeometry(long timestamp, TagInterpreter areaDecider) {
    if (this.isArea(areaDecider)) {
      return getMultiPolygonGeometry(timestamp, areaDecider);
    }
    /*if (this.isLine(areaDecider)) {
      return getMultiLineStringGeometry(timestamp);
    }*/
    GeometryFactory geometryFactory = new GeometryFactory();
    Geometry[] geoms = new Geometry[getMembers().length];
    for (int i = 0; i < getMembers().length; i++) {
      try {
        geoms[i] = getMembers()[i].getEntity().getByTimestamp(timestamp).getGeometry(timestamp, areaDecider);
      } catch (NullPointerException ex) {
        LOG.log(Level.WARNING, "No Entity in Member, Geometry could not be created.", ex);
        return null;
      }
    }
    return geometryFactory.createGeometryCollection(geoms);
  }

  private Geometry getMultiPolygonGeometry(long timestamp, TagInterpreter tagInterpreter) {
    GeometryFactory geometryFactory = new GeometryFactory();

    Stream<OSMWay> outerMembers = this.getMemberEntities(timestamp, tagInterpreter::isMultipolygonOuterMember)
            .map(osm -> (OSMWay) osm)
            .filter(way -> way != null && way.isVisible());

    Stream<OSMWay> innerMembers = this.getMemberEntities(timestamp, tagInterpreter::isMultipolygonInnerMember)
            .map(osm -> (OSMWay) osm)
            .filter(way -> way != null && way.isVisible());

    OSMNode[][] outerLines = outerMembers
            .map(way -> way.getRefs(timestamp)
                    .filter(node -> node != null && node.isVisible())
                    .toArray(OSMNode[]::new)
            ).filter(line -> line.length > 0).toArray(OSMNode[][]::new);
    OSMNode[][] innerLines = innerMembers
            .map(way -> way.getRefs(timestamp)
                    .filter(node -> node != null && node.isVisible())
                    .toArray(OSMNode[]::new)
            ).filter(line -> line.length > 0).toArray(OSMNode[][]::new);

    // construct rings from polygons
    List<LinearRing> outerRings = join(outerLines).stream()
            .map(ring -> geometryFactory.createLinearRing(
                    ring.stream().map(node -> new Coordinate(node.getLongitude(), node.getLatitude())).toArray(Coordinate[]::new)
            )).collect(Collectors.toList());
    List<LinearRing> innerRings = join(innerLines).stream()
            .map(ring -> geometryFactory.createLinearRing(
                    ring.stream().map(node -> new Coordinate(node.getLongitude(), node.getLatitude())).toArray(Coordinate[]::new)
            )).collect(Collectors.toList());

    // construct multipolygon from rings
    // todo: handle nested outers with holes (e.g. inner-in-outer-in-inner-in-outer) - worth the effort? see below for a possibly much easier implementation.
    List<Polygon> polys = outerRings.stream().map(outer -> {
      Polygon outerPolygon = new Polygon(outer, null, geometryFactory);
      List<LinearRing> matchingInners = innerRings.stream().filter(ring -> ring.within(outerPolygon)).collect(Collectors.toList());
      // todo: check for inners containing other inners -> inner-in-outer-in-inner-in-outer case
      return new Polygon(outer, matchingInners.toArray(new LinearRing[matchingInners.size()]), geometryFactory);
    }).collect(Collectors.toList());

    // todo: what to do with unmatched inner rings??
    if (polys.size() == 1) {
      return polys.get(0);
    } else {
      return new MultiPolygon(polys.toArray(new Polygon[polys.size()]), geometryFactory);
    }
  }

  // helper that joins adjacent osm ways into linear rings
  private List<List<OSMNode>> join(OSMNode[][] lines) {
    // make a (mutable) copy of the polygons array
    List<List<OSMNode>> ways = new LinkedList<>();
    for (int i = 0; i < lines.length; i++) {
      ways.add(new LinkedList<>(Arrays.asList(lines[i])));
    }
    List<List<OSMNode>> joined = new LinkedList<>();

    while (!ways.isEmpty()) {
      List<OSMNode> current = ways.remove(0);
      joined.add(current);
      while (!ways.isEmpty()) {
        long firstId = current.get(0).getId();
        long lastId = current.get(current.size() - 1).getId();
        if (firstId == lastId) {
          break; // ring is complete -> we're done
        }
        boolean joinable = false;
        for (int i = 0; i < ways.size(); i++) {
          List<OSMNode> what = ways.get(i);
          if (lastId == what.get(0).getId()) { // end of partial ring matches to start of current line
            what.remove(0);
            current.addAll(what);
            ways.remove(i);
            joinable = true;
            break;
          } else if (firstId == what.get(what.size() - 1).getId()) { // start of partial ring matches end of current line
            what.remove(what.size() - 1);
            current.addAll(0, what);
            ways.remove(i);
            joinable = true;
            break;
          } else if (lastId == what.get(what.size() - 1).getId()) { // end of partial ring matches end of current line
            what.remove(what.size() - 1);
            current.addAll(Lists.reverse(what));
            ways.remove(i);
            joinable = true;
            break;
          } else if (firstId == what.get(0).getId()) { // start of partial ring matches start of current line
            what.remove(0);
            current.addAll(0, Lists.reverse(what));
            ways.remove(i);
            joinable = true;
            break;
          }
        }
        if (!joinable) {
          // Invalid geometry (dangling way, unclosed ring)
          break;
        }
      }
    }

    return joined;
  }

  private Geometry getMultiLineStringGeometry(long timestamp) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String toString() {
    return String.format("Relation-> %s Mem:%s", super.toString(), Arrays.toString(getMembers()));
  }

  @Override
  public String toString(TagTranslator tagTranslator) {
    StringBuilder sb = new StringBuilder();
    sb.append("RELATION-> ").append(super.toString(tagTranslator)).append(" Mem:").append("[");
    for (int i = 0; i < getMembers().length; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("(").append(getMembers()[i].toString(tagTranslator)).append(")");
    }
    sb.append("]");

    return sb.toString();
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

    JsonArrayBuilder JSONMembers = Json.createArrayBuilder();
    for (OSMMember mem : getMembers()) {
      JsonObjectBuilder member = Json.createObjectBuilder();
      member.add("type", mem.getType().toString()).add("ref", mem.getId());
      try {
        member.add("role", tagtranslator.role2String(mem.getRoleId()));
      } catch (NullPointerException ex) {
        LOG.log(Level.WARNING, "The TagTranslator could not resolve the roles. Therefore Integer values will be printed.", ex);
        member.add("role", mem.getRoleId());
      }
      JSONMembers.add(member);
    }

    String result = this.toGeoJSONbuilder(timestamp, tagtranslator, areaDecider).add("members", JSONMembers).build().toString();
    return result;
  }

}
