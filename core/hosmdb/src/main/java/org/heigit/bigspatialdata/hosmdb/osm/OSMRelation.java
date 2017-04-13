package org.heigit.bigspatialdata.hosmdb.osm;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMEntity;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMNode;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMWay;
import org.heigit.bigspatialdata.hosmdb.util.tagInterpreter.TagInterpreter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


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
    return areaDecider.evaluateForLine(this);
  }

  @Override
  public Geometry getGeometry(long timestamp, TagInterpreter areaDecider) {
    if (this.isArea(areaDecider)) {
      return getMultiPolygonGeometry(timestamp, areaDecider);
    }
    if (this.isLine(areaDecider)) {
      return getMultiLineStringGeometry(timestamp);
    }
    return null; // better: exception?
  }

  private Geometry getMultiPolygonGeometry(long timestamp, TagInterpreter tagInterpreter) {
    GeometryFactory geometryFactory = new GeometryFactory();

    OSMWay[] outerMembers = Arrays.stream(this.getMembers())
    .filter(tagInterpreter::isMultipolygonOuterMember)
    .map(outerMember -> (HOSMEntity)outerMember.getData())
    .filter(hosm -> hosm instanceof HOSMWay)
    .map(hosm -> (HOSMWay)hosm)
    .map(hosm -> hosm.getByTimestamp(timestamp))
    .filter(way -> way != null && way.isVisible())
    .toArray(OSMWay[]::new);

    OSMWay[] innerMembers = Arrays.stream(this.getMembers())
    .filter(tagInterpreter::isMultipolygonInnerMember)
    .map(outerMember -> (HOSMEntity)outerMember.getData())
    .filter(hosm -> hosm instanceof HOSMWay)
    .map(hosm -> (HOSMWay)hosm)
    .map(hosm -> hosm.getByTimestamp(timestamp))
    .filter(way -> way != null && way.isVisible())
    .toArray(OSMWay[]::new);

    OSMNode[][] outerLines = Arrays.stream(outerMembers)
    .map(way -> Arrays.stream(way.getRefs())
      .map(nd -> (HOSMNode)nd.getData())
      .map(hosm -> hosm.getByTimestamp(timestamp))
      .map(osm -> (OSMNode)osm)
      .filter(node -> node != null && node.isVisible())
      .toArray(OSMNode[]::new)
    ).toArray(OSMNode[][]::new);
    OSMNode[][] innerLines = Arrays.stream(innerMembers)
    .map(way -> Arrays.stream(way.getRefs())
      .map(nd -> (HOSMNode)nd.getData())
      .map(hosm -> hosm.getByTimestamp(timestamp))
      .map(osm -> (OSMNode)osm)
      .filter(node -> node != null && node.isVisible())
      .toArray(OSMNode[]::new)
    ).toArray(OSMNode[][]::new);

    // construct rings from lines
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
      List<LinearRing> matchingInners = innerRings.stream().filter(ring -> ring.within(outer)).collect(Collectors.toList());
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
    //OSMNode[][] joined = new OSMNode[0][0];
    List<List<OSMNode>> ways = new ArrayList<>();
    for (int i=0; i<lines.length; i++) {
      ways.add(Arrays.asList(lines[i]));
    }
    List<List<OSMNode>> joined = new ArrayList<>();

    while (!ways.isEmpty()) {
      List<OSMNode> current = ways.remove(0); // todo: faster if we "pop()" from the end of the array??
      joined.add(current);
      while (!ways.isEmpty()) {
        long firstId = current.get(0).getId();
        long lastId = current.get(0).getId();
        if (firstId == lastId) break; // ring is complete -> we're done
        for (int i=0; i<ways.size(); i++) {
          List<OSMNode> what = ways.get(i);
          if (lastId == what.get(0).getId()) { // end of partial ring matches to start of current line
            what.remove(0);
            current.addAll(what);
            ways.remove(i);
            break;
          } else if (firstId == what.get(what.size()-1).getId()) { // start of partial ring matches end of current line
            what.remove(what.size() - 1);
            current.addAll(0, what);
            ways.remove(i);
            break;
          } else if (lastId == what.get(what.size()-1).getId()) { // end of partial ring matches end of current line
            what.remove(what.size() - 1);
            current.addAll(Lists.reverse(what));
            ways.remove(i);
            break;
          } else if (firstId == what.get(0).getId()) { // start of partial ring matches start of current line
            what.remove(0);
            current.addAll(0, Lists.reverse(what));
            ways.remove(i);
            break;
          }
        }
      }
    }

    return joined;
  }


  private Geometry getMultiLineStringGeometry(long timestamp) {
    throw new NotImplementedException();
  }

}
