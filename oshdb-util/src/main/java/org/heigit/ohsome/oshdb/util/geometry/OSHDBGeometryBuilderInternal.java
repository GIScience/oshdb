package org.heigit.ohsome.oshdb.util.geometry;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.heigit.ohsome.oshdb.OSHDBTemporal;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osh.OSHEntities;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedPolygon;
import org.locationtech.jts.index.strtree.STRtree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds implementations of geometry building algorithms.
 */
public class OSHDBGeometryBuilderInternal {
  private static final Logger LOG = LoggerFactory.getLogger(OSHDBGeometryBuilderInternal.class);
  private final TagInterpreter areaDecider;
  private final GeometryFactory geometryFactory;

  public OSHDBGeometryBuilderInternal(TagInterpreter areaDecider, GeometryFactory geometryFactory) {
    this.areaDecider = areaDecider;
    this.geometryFactory = geometryFactory;
  }

  public OSHDBGeometryBuilderInternal(TagInterpreter areaDecider) {
    this(areaDecider, new GeometryFactory());
  }


  /**
   * Holds auxiliary data for geometry building methods when pre-resolved references are available.
   *
   * <p>This makes it possible to more easily generate geometries from "raw" osm entities, when
   * their referenced entities are already known for a specific timestamp.</p>
   *
   * @param childEntityData The directly referenced child entities of an OSMEntity: for a way,
   *                        this would be a list of OSMNodes, for a relation an Array of OSMWays.
   *                        The order of the array items must correspond to the getMembers() of the
   *                        original entity.
   * @param childWayNodesData The indirectly referenced child-child entities of an OSMRelation. The
   *                          order of the array items must correspond to the getMembers() of the
   *                          original entity.
   */
  public record AuxiliaryData(
      @Nonnull List<? extends OSMEntity> childEntityData,
      @Nullable List<List<OSMNode>> childWayNodesData
  ) {}


  @Nonnull
  Geometry getGeometry(
      OSMEntity entity,
      @Nullable OSHDBTimestamp timestamp,
      @Nullable AuxiliaryData auxiliaryData
  ) {
    if (timestamp != null && OSHDBTemporal.compare(timestamp, entity) < 0) {
      throw new AssertionError(
          "cannot produce geometry of entity for timestamp before this entity's version's timestamp"
      );
    }
    if (entity instanceof OSMNode node) {
      return getNodeGeometry(node);
    } else if (entity instanceof OSMWay way) {
      return getWayGeometry(way,
          timestamp,
          auxiliaryData);
    } else if (entity instanceof OSMRelation relation) {
      return getRelationGeometry(relation,
          timestamp,
          auxiliaryData);
    } else {
      throw new IllegalStateException(
          "entity must be an instance of either OSMNode, OSMWay, or OSMRelation");
    }
  }


  /**
   * Returns the geometry of an OSMEntity when its referenced child (and child-child) entities
   * are known.
   *
   * @param entity the entity to generate the geometry for
   * @param auxiliaryData the child entities the original OSM entity references (at the
   *                      timestamp one is interested in).
   * @return a JTS geometry object
   */
  @Nonnull
  public Geometry getGeometry(
      OSMEntity entity,
      @Nonnull AuxiliaryData auxiliaryData
  ) {
    return getGeometry(
        entity,
        null,
        auxiliaryData);
  }


  /**
   * Construct the geometry of an OSMNode.
   *
   * @param node the node to construct the geometry of
   * @return the geometry as a JTS point
   */
  public Geometry getNodeGeometry(OSMNode node) {
    if (!node.isVisible()) {
      OSHDBGeometryBuilderInternal.LOG.info(
          "node/{} is deleted - falling back to empty (line) geometry", node.getId());
      return geometryFactory.createEmpty(0);
    } else {
      return geometryFactory.createPoint(new Coordinate(node.getLongitude(), node.getLatitude()));
    }
  }


  /**
   * Construct the geometry of an OSMWay, given a timestamp for which the way's references are
   * to be resolved to.
   *
   * @param way the way to construct the geometry of
   * @param timestamp the timestamp at which to resolve the way's referenced nodes
   * @return the geometry as a JTS line string or polygon
   */
  public Geometry getWayGeometry(
      OSMWay way,
      OSHDBTimestamp timestamp) {
    var members = way.getMemberEntities(timestamp);
    return getWayGeometry(way, members);
  }

  /**
   * Construct the geometry of an OSMWay, given the nodes it references as auxiliary data.
   *
   * @param way the way to construct the geometry of
   * @param auxiliaryData the auxiliary data containing the nodes the way references. Must contain
   *                      a list of OSMNodes in the childEntityData property.
   * @return the geometry as a JTS line string or polygon
   */
  public Geometry getWayGeometry(
      OSMWay way,
      @Nonnull AuxiliaryData auxiliaryData) {
    return getWayGeometry(
        way,
        auxiliaryData.childEntityData().stream());
  }

  private Geometry getWayGeometry(
      OSMWay way,
      OSHDBTimestamp timestamp,
      @Nullable AuxiliaryData auxiliaryData
  ) {
    if (!way.isVisible()) {
      OSHDBGeometryBuilderInternal.LOG.info(
          "way/{} is deleted - falling back to empty (line) geometry", way.getId());
      return geometryFactory.createEmpty(1);
    }
    if (auxiliaryData != null) {
      return getWayGeometry(
          way,
          auxiliaryData);
    } else {
      return getWayGeometry(
          way,
          timestamp);
    }
  }

  /**
   * Construct the geometry of an OSMWay, given the nodes it references as auxiliary data.
   *
   * @param way the way to construct the geometry of
   * @param resolvedMembers a stream of the nodes which the way references
   * @return the geometry as a JTS line string or polygon
   */
  public Geometry getWayGeometry(
      OSMWay way,
      Stream<? extends OSMEntity> resolvedMembers) {
    // todo: handle old-style multipolygons here???
    Coordinate[] coords = resolvedMembers
        .filter(Objects::nonNull)
        .filter(OSMEntity::isVisible)
        .map(OSMNode.class::cast)
        .map(OSHDBGeometryBuilder::getCoordinate)
        .toArray(Coordinate[]::new);
    if (areaDecider.isArea(way)) {
      if (coords.length >= 4 && coords[0].equals2D(coords[coords.length - 1])) {
        return geometryFactory.createPolygon(coords);
      } else {
        LOG.warn("way/{} doesn't form a linear ring - falling back to linestring", way.getId());
      }
    }
    if (coords.length >= 2) {
      return geometryFactory.createLineString(coords);
    }
    if (coords.length == 1) {
      LOG.info("way/{} is single-noded - falling back to point geometry", way.getId());
      return geometryFactory.createPoint(coords[0]);
    } else {
      LOG.warn("way/{} with no nodes - falling back to empty (point) geometry", way.getId());
      return geometryFactory.createEmpty(0);
    }
  }

  private Geometry getRelationGeometry(
      OSMRelation relation,
      OSHDBTimestamp timestamp,
      AuxiliaryData auxiliaryData
  ) {
    if (!relation.isVisible()) {
      OSHDBGeometryBuilderInternal.LOG.info(
          "relation/{} is deleted - falling back to empty geometry (collection)",
          relation.getId());
      return geometryFactory.createEmpty(-1);
    }
    if (areaDecider.isArea(relation)) {
      try {
        Geometry multipolygon = getMultiPolygonGeometry(
                relation,
                timestamp,
                auxiliaryData
        );
        if (!multipolygon.isEmpty()) {
          return multipolygon;
        }
        // otherwise (empty geometry): fall back to geometry collection builder
      } catch (IllegalArgumentException e) {
        // fall back to geometry collection builder
      }
    }
    /* todo:implement multilinestring mode for stuff like route relations
     * if (areaDecider.isLine(entity)) { return getMultiLineStringGeometry(timestamp); }
     */
    return getGeometryCollectionGeometry(
            relation,
            timestamp,
            auxiliaryData);
  }

  /**
   * Construct the geometry of an OSMRelation, interpreted as a GeometryCollection geometry type,
   * given a timestamp for which the relation's references are to be resolved to.
   *
   * @param relation the relation to construct the geometry of
   * @param timestamp the timestamp at which to resolve the relation's referenced nodes
   * @return the geometry as a JTS geometry collection
   */
  public Geometry getGeometryCollectionGeometry(
      OSMRelation relation,
      OSHDBTimestamp timestamp
  ) {
    return getGeometryCollectionGeometry(
        relation,
        timestamp,
        null);
  }

  /**
   * Construct the geometry of an OSMRelation, interpreted as a GeometryCollection geometry type,
   * given the entities it references as auxiliary data.
   *
   * @param relation the relation to construct the geometry of
   * @param auxiliaryData the auxiliary data containing the referenced ways and way-nodes.
   *                      Must contain a list of OSMWays in the childEntityData property and
   *                      a list of lists of OSMNodes in the childWayNodesData property.
   * @return the geometry as a JTS geometry collection
   */
  public Geometry getGeometryCollectionGeometry(
      OSMRelation relation,
      @Nonnull AuxiliaryData auxiliaryData
  ) {
    return getGeometryCollectionGeometry(
        relation,
        null,
        auxiliaryData);
  }

  private Geometry getGeometryCollectionGeometry(
      OSMRelation relation,
      OSHDBTimestamp timestamp,
      @Nullable AuxiliaryData auxiliaryData
  ) {
    OSMMember[] relationMembers = relation.getMembers();
    Geometry[] geoms = new Geometry[relationMembers.length];
    boolean completeGeometry = true;
    for (int i = 0; i < relationMembers.length; i++) {
      OSHEntity memberOSHEntity = relationMembers[i].getEntity();
      OSMEntity memberEntity;
      if (auxiliaryData != null) {
        memberEntity = auxiliaryData.childEntityData().get(i);
      } else {
        // memberOSHEntity might be null when working on an extract with incomplete relation members
        memberEntity = memberOSHEntity == null ? null :
            OSHEntities.getByTimestamp(memberOSHEntity, timestamp);
      }
      /*
      memberEntity might be null when working with redacted data, for example:
       - user 1 creates node 1 (timestamp 1)
       - user 2 creates relation 1 with node 1 as member (timestamp 2)
       - user 2 edits node 1, which now has a version 2 (timestamp 3)
       - user 1's edits are redacted -> node 1 version 1 is now hidden (version 2 isn't)
      now when requesting relation 1's geometry at timestamps between 2 and 3, memberOSHEntity
      is not null, but memberEntity is.
      */
      if (memberEntity == null) {
        geoms[i] = null;
        completeGeometry = false;
        LOG.info(
            "Member entity {}/{} of relation/{} missing, geometry could not be assembled fully.",
            relationMembers[i].getType(), relationMembers[i].getId(), relation.getId()
        );
      } else {
        AuxiliaryData subAuxiliaryData = null;
        if (auxiliaryData != null && auxiliaryData.childWayNodesData() != null) {
          var childWayNodesData = auxiliaryData.childWayNodesData();
          subAuxiliaryData = new AuxiliaryData(childWayNodesData.get(i), null);
        }
        geoms[i] = getGeometry(
            memberEntity,
            timestamp,
            subAuxiliaryData);
      }
    }
    if (completeGeometry) {
      return geometryFactory.createGeometryCollection(geoms);
    } else {
      return geometryFactory.createGeometryCollection(
          Arrays.stream(geoms).filter(Objects::nonNull).toArray(Geometry[]::new)
      );
    }
  }


  /**
   * Construct the geometry of an OSMRelation, interpreted as a MultiPolygon geometry type,
   * given a timestamp for which the relation's references are to be resolved to.
   *
   * @param relation the relation to construct the geometry of
   * @param timestamp the timestamp at which to resolve the relation's referenced nodes
   * @return the geometry as a JTS multi polygon
   */
  public Geometry getMultiPolygonGeometry(
      OSMRelation relation,
      OSHDBTimestamp timestamp
  ) {
    List<LinkedList<OSMNode>> outerLines = waysToLines(
        relation.getMemberEntities(timestamp, areaDecider::isMultipolygonOuterMember),
        timestamp
    );

    List<LinkedList<OSMNode>> innerLines = waysToLines(
        relation.getMemberEntities(timestamp, areaDecider::isMultipolygonInnerMember),
        timestamp
    );

    return getMultiPolygonGeometry(
        outerLines,
        innerLines
    );
  }

  /**
   * Construct the geometry of an OSMRelation, interpreted as a MultiPolygon geometry type,
   * given the entities it references as auxiliary data.
   *
   * @param relation the relation to construct the geometry of
   * @param auxiliaryData the auxiliary data containing the referenced ways and way-nodes.
   *                      Must contain a list of OSMWays in the childEntityData property and
   *                      a list of lists of OSMNodes in the childWayNodesData property.
   * @return the geometry as a JTS multi polygon
   */
  public Geometry getMultiPolygonGeometry(
      OSMRelation relation,
      @Nonnull AuxiliaryData auxiliaryData
  ) {
    return getMultiPolygonGeometry(
        relation,
        auxiliaryData.childEntityData(),
        auxiliaryData.childWayNodesData());
  }

  Geometry getMultiPolygonGeometry(
      OSMRelation relation,
      OSHDBTimestamp timestamp,
      @Nullable AuxiliaryData auxiliaryData
  ) {
    if (auxiliaryData != null) {
      return getMultiPolygonGeometry(
          relation,
          auxiliaryData);
    } else {
      return getMultiPolygonGeometry(
          relation,
          timestamp);
    }
  }

  private Geometry getMultiPolygonGeometry(
      OSMRelation relation,
      List<? extends OSMEntity> waysData,
      List<List<OSMNode>> waysNodesData
  ) {
    List<LinkedList<OSMNode>> outerLines = new LinkedList<>();
    List<LinkedList<OSMNode>> innerLines = new LinkedList<>();

    var members = relation.getMembers();
    for (var i = 0; i < members.length; i++) {
      var member = members[i];
      var memberData = waysData.get(i);
      if (memberData == null || !memberData.isVisible()) {
        continue;
      }

      var line = waysNodesData.get(i).stream()
          .filter(Objects::nonNull)
          .filter(OSMEntity::isVisible)
          .collect(Collectors.toCollection(LinkedList::new));

      if (areaDecider.isMultipolygonOuterMember(member)) {
        outerLines.add(line);
      } else if (areaDecider.isMultipolygonInnerMember(member)) {
        innerLines.add(line);
      }
    }

    return getMultiPolygonGeometry(
        outerLines,
        innerLines
    );
  }

  /**
   * Construct the geometry of an OSMRelation, interpreted as a MultiPolygon geometry type,
   * given the line segements of the outer and inner rings it consists of.
   *
   * @param outerLines A list of segments which can be glued into 1 to n closed rings which form
   *                   the outer shells of the multi polygon
   * @param innerLines A list of segments which can be glued into 0 to n closed rings which form
   *                   the inner holes of the multi polygon
   * @return
   */
  public Geometry getMultiPolygonGeometry(
      List<LinkedList<OSMNode>> outerLines,
      List<LinkedList<OSMNode>> innerLines
  ) {
    // construct inner and outer rings
    List<LinkedList<OSMNode>> outerRingsNodes = buildRings(outerLines);
    List<LinkedList<OSMNode>> innerRingsNodes = buildRings(innerLines);

    // check if there are any pinched off sections in outer rings
    splitPinchedRings(outerRingsNodes, innerRingsNodes);
    // check if there are any touching inner/outer rings, merge any
    mergeTouchingRings(innerRingsNodes);
    // create JTS rings for non-degenerate rings only

    List<LinearRing> outerRings = outerRingsNodes.stream()
        .filter(ring -> ring.size() >= LinearRing.MINIMUM_VALID_SIZE)
        .map(ring -> geometryFactory.createLinearRing(
            ring.stream().map(node -> new Coordinate(node.getLongitude(), node.getLatitude()))
                .toArray(Coordinate[]::new)))
        .toList();
    List<LinearRing> innerRings = innerRingsNodes.stream()
        .filter(ring -> ring.size() >= LinearRing.MINIMUM_VALID_SIZE)
        .map(ring -> geometryFactory.createLinearRing(
            ring.stream().map(node -> new Coordinate(node.getLongitude(), node.getLatitude()))
                .toArray(Coordinate[]::new)))
        .toList();

    // construct multipolygon from rings
    // todo: handle nested outers with holes (e.g. inner-in-outer-in-inner-in-outer) - worth the
    // effort? see below for a possibly much easier implementation.
    Geometry result;
    if (outerRings.size() == 1) {
      result = geometryFactory.createPolygon(
          outerRings.get(0),
          innerRings.toArray(new LinearRing[0])
      );
    } else {
      STRtree innersTree = new STRtree();
      innerRings.forEach(inner -> innersTree.insert(inner.getEnvelopeInternal(), inner));
      Polygon[] polys = outerRings.stream().map(outer -> {
        // todo: check for inners containing other inners -> inner-in-outer-in-inner-in-outer case
        try {
          return constructMultipolygonPart(
              innersTree,
              geometryFactory.createPolygon(outer)
          );
        } catch (TopologyException e) {
          // try again with buffer(0) on outer ring
          Geometry buffered = geometryFactory.createPolygon(outer).buffer(0);
          if (buffered instanceof Polygon polygon) {
            return constructMultipolygonPart(
                innersTree,
                polygon
            );
          } else {
            return null;
          }
        }
      })
      .filter(Objects::nonNull)
      .toArray(Polygon[]::new);
      // todo: what to do with unmatched inner rings??
      result = geometryFactory.createMultiPolygon(polys);
    }
    return result;
  }

  private static List<LinkedList<OSMNode>> waysToLines(
      Stream<OSMEntity> members, OSHDBTimestamp timestamp) {
    return members
        .map(OSMWay.class::cast)
        .filter(Objects::nonNull)
        .filter(OSMEntity::isVisible)
        .map(way -> way.getMemberEntities(timestamp)
            .filter(Objects::nonNull)
            .filter(OSMEntity::isVisible)
            .collect(Collectors.toCollection(LinkedList::new))
        )
        .collect(Collectors.toCollection(LinkedList::new));
  }

  /**
   * Search and merge touching rings.
   *
   * <p>Attention: modifies the input data, such that there are no more rings that touch in
   * one or more segments.</p>
   *
   * <p>
   *   Touching rings are defined as rings which share at least one segment (a segment is formed by
   *   two consecutive ring nodes, regardless of their order). An example is:
   *   [r1 = (A,B,C,D,E,F,A); r2 = (X,Y,B,C,D,E,X)].
   *   The result would be: [r1 = (B,A,F,E,X,Y,B)] "or any equivalent representation of this ring"
   * </p>
   *
   * <pre>
   * F--E----X       F--E----X
   * |  |    |       |       |
   * |  D-C  |  -->  |       |
   * |    |  |       |       |
   * A----B--Y       A----B--Y
   * </pre>
   *
   * @param ringsNodes a collection of node-lists, each forming a ring (i.e. a closed linestring)
   */
  private static void mergeTouchingRings(Collection<LinkedList<OSMNode>> ringsNodes) {
    // ringSegments will hold a reference of which ring a particular segment is part of.
    // Note that in the final result, each segment will be "used" by exactly one ring.
    Map<Segment, LinkedList<OSMNode>> ringSegments = new HashMap<>();
    for (Iterator<LinkedList<OSMNode>> ringsIter = ringsNodes.iterator(); ringsIter.hasNext();) {
      LinkedList<OSMNode> ringNodes = ringsIter.next();
      // will contain the list of segments of the current or merged ring.
      // after the merging process, these are used to populate the ringSegments map.
      List<Segment> mergedRingSegments = new ArrayList<>(ringNodes.size() - 1);
      // pairwise iterate over nodes of current ring ->
      Iterator<OSMNode> ringNodesIter = ringNodes.iterator();
      long prevNodeId = ringNodesIter.next().getId();
      while (ringNodesIter.hasNext()) {
        long thisNodeId = ringNodesIter.next().getId();
        Segment segment = new Segment(prevNodeId, thisNodeId);
        prevNodeId = thisNodeId;
        if (!ringSegments.containsKey(segment)) {
          // we have not encountered this segment yet -> just remember it for later
          mergedRingSegments.add(segment);
        } else {
          // we have already seen this segment:
          // merge this ring (ringNodes) into the previously encountered one (targetNodes)
          LinkedList<OSMNode> targetNodes = ringSegments.get(segment);
          // remove all segments pointing to the target ring, as we will rebuild it from scratch
          ringSegments.values().removeAll(Collections.singleton(targetNodes));
          // cut and rewind target and current rings to the matching segment we found
          cutAtSegment(targetNodes, segment);
          cutAtSegment(ringNodes, segment);
          // cut back all other segments which are shared by current and target ring
          mergeSegmentsToRing(targetNodes, ringNodes);
          // clean up
          // add merged ring's segments to segments->ring map
          mergedRingSegments.clear();
          Iterator<OSMNode> targetNodesIter = targetNodes.iterator();
          long segmentPrevNodeId = targetNodesIter.next().getId();
          while (targetNodesIter.hasNext()) {
            long segmentCurrNodeId = targetNodesIter.next().getId();
            mergedRingSegments.add(new Segment(segmentPrevNodeId, segmentCurrNodeId));
            segmentPrevNodeId = segmentCurrNodeId;
          }
          // remove current ring from end result, as it was merged with another ring already.
          ringsIter.remove();
          // save target ring for global segments->ring map (ringSegments)
          ringNodes = targetNodes;
          // abort current ring, continue with next one
          break;
        }
      }
      // add current ring's segments to map of all already processed segments
      for (Segment mergedRingSegment : mergedRingSegments) {
        ringSegments.put(mergedRingSegment, ringNodes);
      }
    }
  }



  /**
   * Search and split self-intersecting/pinched/figure-8 rings.
   *
   * <p>Attention: modifies the input data, such that there are no more figure-8 rings.</p>
   *
   * <p>
   *   A pinched ring forms a figure-8 configuration where the ring touches itself in a single
   *   point. An example is: [r = (A,B,C,D,E,F,C,G,A)].
   *   The result would be: [r1 = (C,D,E,G,C); r2 = (A,B,C,G,A)].
   * </p>
   *
   * <pre>
   *  A--B
   *  |  |
   *  G--C--D
   *     |  |
   *     F--E
   * </pre>
   *
   * @param ringsNodes a collection of node-lists, each forming a ring (i.e. a closed linestring)
   * @param holeRingsNodes a collection where holes formed by "upended" figure-8's should be stored
   */
  private void splitPinchedRings(
      Collection<LinkedList<OSMNode>> ringsNodes,
      Collection<LinkedList<OSMNode>> holeRingsNodes
  ) {
    Map<Long, Integer> nodeIds = new HashMap<>();
    Collection<LinkedList<OSMNode>> additionalRings = new LinkedList<>();
    for (LinkedList<OSMNode> ringNodes : ringsNodes) {
      var splitRings = splitPinchedRing(ringNodes, nodeIds);
      if (splitRings != null) {
        // if self-intersection(s) were found, we need to check whether these are next to or
        // overlapping each other. to do this, we convert the rings to polygon geometries first
        splitRings.add(new LinkedList<>(ringNodes));
        ringNodes.clear();
        var splitRingsGeoms = splitRings.stream()
            .map(ring -> {
              if (ring.size() >= LinearRing.MINIMUM_VALID_SIZE) {
                return geometryFactory.createPolygon(ring.stream()
                    .map(node -> new Coordinate(node.getLongitude(), node.getLatitude()))
                    .toArray(Coordinate[]::new));
              } else {
                return geometryFactory.createPolygon();
              }
            })
            .toList();
        // determine which of the rings is "coveredBy" how many of the others
        var nestingNumbers = Collections.nCopies(splitRingsGeoms.size(), 0)
            .toArray(new Integer [] {});
        for (var i = 0; i < splitRingsGeoms.size(); i++) {
          for (var j = 0; j < splitRingsGeoms.size(); j++) {
            if (i == j) {
              continue;
            }
            if (splitRingsGeoms.get(i).coveredBy(splitRingsGeoms.get(j))) {
              nestingNumbers[i]++;
            }
          }
        }
        // sort result into (additional) rings and holes
        for (var i = 0; i < splitRingsGeoms.size(); i++) {
          if (nestingNumbers[i] % 2 == 0) {
            additionalRings.add(splitRings.get(i));
          } else {
            holeRingsNodes.add(splitRings.get(i));
          }
        }
      }
    }
    ringsNodes.addAll(additionalRings);
  }

  /**
   * Search and split pinched (figure-8) rings.
   *
   * @return null if no self-intersection is found,
   *         otherwise a collection containing additional split-off rings
   */
  private static List<LinkedList<OSMNode>> splitPinchedRing(
      LinkedList<OSMNode> ringNodes,
      Map<Long, Integer> nodeIds
  ) {
    List<LinkedList<OSMNode>> result = null;
    boolean wasSplittable;
    do {
      wasSplittable = false;
      nodeIds.clear();
      var currentNodePos = 0;
      for (OSMNode ringNode : ringNodes) {
        long nodeId = ringNode.getId();
        if (nodeIds.containsKey(nodeId)) {
          // split off ring between previous and current ring position
          int nodePos = nodeIds.get(nodeId);
          final var additionalRing =
              new LinkedList<>(ringNodes.subList(nodePos, currentNodePos + 1));
          final var remainingRing = new LinkedList<OSMNode>();
          remainingRing.addAll(ringNodes.subList(0, nodePos));
          remainingRing.addAll(ringNodes.subList(currentNodePos, ringNodes.size()));
          wasSplittable = true;
          // add to results
          ringNodes.clear();
          ringNodes.addAll(remainingRing);
          if (result == null) {
            result = new ArrayList<>();
          }
          result.add(additionalRing);
          break;
        }
        if (currentNodePos > 0) {
          // don't memorize start node, since it is always repeated at the end of the ring
          nodeIds.put(nodeId, currentNodePos);
        }
        currentNodePos++;
      }
      // repeat until the ring doesn't have any more self intersections
    } while (wasSplittable);
    return result;
  }

  /**
   * Cut a ring at the given segment.
   *
   * <p>The result is stored in the input variable (modified in-place).</p>
   *
   * <p>
   *   After cutting of a ring, one gets an open line string with the ends corresponding exactly
   *   to the cut-segments nodes.
   *   Example: ring = (A,B,C,D,E,F,A); cut = (B,C); result = (C,D,E,F,A,B)
   * </p>
   *
   * <pre>
   * F--E         F--E
   * |  |         |  |
   * |  D-C  -->  |  D-C
   * |    |       |
   * A----B       A----B
   * </pre>
   *
   * @param ring a ring of nodes
   * @param cutSegment the segment where to cut at
   */
  private static void cutAtSegment(LinkedList<OSMNode> ring, Segment cutSegment) {
    // split the ring open, by removing the "redundant" coordinate.
    // example: (A,B,C,D,E,F,A) -> (B,C,D,E,F,A)
    ring.removeFirst();
    for (int i = 0; i < ring.size(); i++) {
      // do the open ends of the current ring match the cut segment?
      Segment splitSegment = new Segment(ring.getFirst().getId(), ring.getLast().getId());
      if (cutSegment.equals(splitSegment)) {
        // yes -> we're done
        return;
      } else {
        // no -> wind the split location in the input ring one node forward
        // example: (B,C,D,E,F,A) -> (C,D,E,F,A,B) -- split segment was (B,A) and is now (C,B)
        ring.add(ring.removeFirst());
      }
    }
    assert false : "cut segment not found in ring";
    throw new IllegalStateException("cut segment not found in ring");
  }

  /**
   * Take two open line strings (which share a common pair of start/end nodes) and merge them into
   * a single ring without any degeneracies.
   *
   * <p>The result is stored in the target input variable (both inputs are modified in-place).</p>
   *
   * <p>
   *   After joining of a ring, one gets a closed ring with no back-tracking segments.
   *   Example: target = (B,C,D,E,F,A); source = (C,D,E,X,Y,B)
   *            result (in target) = (B,A,F,E,X,Y,B) or any equivalent representation of this ring
   * </p>
   *
   * <pre>
   * F--E       E----X       F--E----X
   * |  |       |    |       |       |
   * |  D-C  +  D-C  |  -->  |       |
   * |               |       |       |
   * A----B       B--Y       A----B--Y
   * </pre>
   *
   * @param target a ring which has been cut open using {@link #cutAtSegment(LinkedList, Segment)}
   * @param source a ring which has been cut open using {@link #cutAtSegment(LinkedList, Segment)}
   */
  private static void mergeSegmentsToRing(LinkedList<OSMNode> target, LinkedList<OSMNode> source) {
    // make sure source and target are pointing in opposite order:
    // this facilitates merging them into a closed loop in the end of this method
    if (target.getFirst().getId() == source.getFirst().getId()) {
      Collections.reverse(source);
    }
    // shave off shared segments between both rings
    while (source.size() > 1 && target.size() > 1
        && source.getFirst().getId() == target.getLast().getId()
        && source.get(1).getId() == target.get(target.size() - 2).getId()) {
      source.removeFirst();
      target.removeLast();
    }
    while (source.size() > 1 && target.size() > 1
        && source.getLast().getId() == target.getFirst().getId()
        && source.get(source.size() - 2).getId() == target.get(1).getId()) {
      source.removeLast();
      target.removeFirst();
    }
    // merge two halve rings to form a new complete one
    source.removeFirst();
    target.addAll(source);
  }

  private Polygon constructMultipolygonPart(
      STRtree inners,
      Polygon outer
  ) throws TopologyException {
    PreparedGeometry outerPolygon = new PreparedPolygon(outer);
    @SuppressWarnings("unchecked") // JTS returns raw types, but they are actually LinearRings
        List<LinearRing> innerCandidates = inners.query(outer.getEnvelopeInternal());
    return geometryFactory.createPolygon(
        outer.getExteriorRing(),
        innerCandidates.stream().filter(outerPolygon::contains).toArray(LinearRing[]::new)
    );
  }

  /**
   * Helper that joins adjacent osm ways into linear rings.
   *
   * <p>Mutates the input lists.</p>
   */
  private static List<LinkedList<OSMNode>> buildRings(
      List<LinkedList<OSMNode>> ways
  ) {
    List<LinkedList<OSMNode>> joined = new LinkedList<>();
    // iterate until there are no more ways left to process
    while (!ways.isEmpty()) {
      LinkedList<OSMNode> current = ways.remove(0);
      if (current.isEmpty()) {
        continue;
      }
      // iterate until the way cannot be joined to another way
      boolean joinable;
      do {
        long firstId = current.getFirst().getId();
        long lastId = current.getLast().getId();
        if (firstId == lastId) {
          // ring is complete -> we are done
          joined.add(current);
          break;
        }
        joinable = false;
        for (var waysIterator = ways.iterator(); waysIterator.hasNext();) {
          LinkedList<OSMNode> what = waysIterator.next();
          if (what.isEmpty()) {
            continue;
          }
          if (lastId == what.getFirst().getId()) {
            // end of partial ring matches to start of current line
            what.removeFirst();
            current.addAll(what);
            waysIterator.remove();
            lastId = current.getLast().getId();
            joinable = true;
          } else if (firstId == what.getLast().getId()) {
            // start of partial ring matches end of current line
            what.removeLast();
            current.addAll(0, what);
            waysIterator.remove();
            firstId = current.getFirst().getId();
            joinable = true;
          } else if (lastId == what.getLast().getId()) {
            // end of partial ring matches end of current line
            what.removeLast();
            current.addAll(Lists.reverse(what));
            waysIterator.remove();
            lastId = current.getLast().getId();
            joinable = true;
          } else if (firstId == what.getFirst().getId()) {
            // start of partial ring matches start of current line
            what.removeFirst();
            current.addAll(0, Lists.reverse(what));
            waysIterator.remove();
            firstId = current.getFirst().getId();
            joinable = true;
          }
          if (firstId == lastId) {
            break;
          }
        }
        // joinable==false for invalid geometries (dangling way, unclosed ring)
      } while (joinable);
    }

    return joined;
  }

  private static class Segment {
    long id1;
    long id2;

    Segment(long id1, long id2) {
      this.id1 = id1;
      this.id2 = id2;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof Segment otherSegment) {
        return otherSegment.id1 == this.id1 && otherSegment.id2 == this.id2
            || otherSegment.id1 == this.id2 && otherSegment.id2 == this.id1;
      } else {
        return super.equals(other);
      }
    }

    @Override
    public int hashCode() {
      return (int) this.id1 + (int) this.id2;
    }
  }
}
