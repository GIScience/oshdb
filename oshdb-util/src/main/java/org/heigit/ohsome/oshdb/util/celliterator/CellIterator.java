package org.heigit.ohsome.oshdb.util.celliterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.index.XYGrid;
import org.heigit.ohsome.oshdb.osh.OSHEntities;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.function.OSHEntityFilter;
import org.heigit.ohsome.oshdb.util.function.OSMEntityFilter;
import org.heigit.ohsome.oshdb.util.geometry.Geo;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.fip.FastBboxInPolygon;
import org.heigit.ohsome.oshdb.util.geometry.fip.FastBboxOutsidePolygon;
import org.heigit.ohsome.oshdb.util.geometry.fip.FastPolygonOperations;
import org.heigit.ohsome.oshdb.util.osh.OSHEntityTimeUtils;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestampInterval;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;
import org.locationtech.jts.geom.TopologyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows to iterate through the contents of OSH grid cells.
 *
 * <p>There are two modes of iterating through a cell: 1) iterate at specific timestamps
 * (entity snapshots) {@link #iterateByTimestamps(GridOSHEntity)} or 2) iterate through
 * all (minor) versions of each entity {@link #iterateByContribution(GridOSHEntity)}.
 */
public class CellIterator implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(CellIterator.class);

  private final TreeSet<OSHDBTimestamp> timestamps;
  private final OSHDBBoundingBox boundingBox;
  private boolean isBoundByPolygon;
  private FastBboxInPolygon bboxInPolygon;
  private FastBboxOutsidePolygon bboxOutsidePolygon;
  private FastPolygonOperations fastPolygonClipper;
  private final TagInterpreter tagInterpreter;
  private final OSHEntityFilter oshEntityPreFilter;
  private final OSMEntityFilter osmEntityFilter;
  private final boolean includeOldStyleMultipolygons;

  /**
   * Creates a cell iterator from a bounding box and a bounding polygon.
   *
   * @param timestamps a list of timestamps to return data for
   * @param boundingBox only entities inside or intersecting this bbox are returned, geometries are
   *        clipped to this extent
   * @param tagInterpreter helper object which is used to determine entitie's geometry types
   * @param boundingPolygon only entities inside or intersecting this polygon are returned,
   *        geometries are clipped to this extent. If present, the supplied boundingBox must be the
   *        boundingBox of this polygon
   * @param oshEntityPreFilter (optional) a lambda called for each osh entity to pre-filter
   *        elements. only if it returns true, its osmEntity objects can be included in the output
   * @param osmEntityFilter a lambda called for each entity. if it returns true, the particular
   *        feature is included in the output
   * @param includeOldStyleMultipolygons if true, output contains also data for "old style
   *        multipolygons".
   *        Note, that if includeOldStyleMultipolygons is true, for each old style multipolygon only
   *        the geometry of the inner holes are returned (while the outer part is already present as
   *        the respective way's output)! This has to be interpreted separately and differently in
   *        the data analysis! The includeOldStyleMultipolygons is also quite a bit less efficient
   *        (both CPU and memory) as the default path.
   * @param <P> either {@link org.locationtech.jts.geom.Polygon} or
   *        {@link org.locationtech.jts.geom.MultiPolygon}
   */
  public <P extends Geometry & Polygonal> CellIterator(
      SortedSet<OSHDBTimestamp> timestamps,
      OSHDBBoundingBox boundingBox,
      P boundingPolygon,
      TagInterpreter tagInterpreter,
      OSHEntityFilter oshEntityPreFilter,
      OSMEntityFilter osmEntityFilter,
      boolean includeOldStyleMultipolygons
  ) {
    this(
        timestamps,
        boundingBox,
        tagInterpreter,
        oshEntityPreFilter,
        osmEntityFilter,
        includeOldStyleMultipolygons
    );
    if (boundingPolygon != null) {
      this.isBoundByPolygon = true;
      this.bboxInPolygon = new FastBboxInPolygon(boundingPolygon);
      this.bboxOutsidePolygon = new FastBboxOutsidePolygon(boundingPolygon);
      this.fastPolygonClipper = new FastPolygonOperations(boundingPolygon);
    }
  }

  /**
   * Creates a cell iterator from a bounding polygon.
   *
   * @param timestamps a list of timestamps to return data for
   * @param tagInterpreter helper object which is used to determine entitie's geometry types
   * @param boundingPolygon only entities inside or intersecting this polygon are returned,
   *        geometries are clipped to this extent.
   * @param oshEntityPreFilter (optional) a lambda called for each osh entity to pre-filter
   *        elements. only if it returns true, its osmEntity objects can be included in the output
   * @param osmEntityFilter a lambda called for each entity. if it returns true, the particular
   *        feature is included in the output
   * @param includeOldStyleMultipolygons if true, output contains also data for "old style
   *        multipolygons".
   *        Note, that if includeOldStyleMultipolygons is true, for each old style multipolygon only
   *        the geometry of the inner holes are returned (while the outer part is already present as
   *        the respective way's output)! This has to be interpreted separately and differently in
   *        the data analysis! The includeOldStyleMultipolygons is also quite a bit less efficient
   *        (both CPU and memory) as the default path.
   * @param <P> either {@link org.locationtech.jts.geom.Polygon} or
   *        {@link org.locationtech.jts.geom.MultiPolygon}
   */
  public <P extends Geometry & Polygonal> CellIterator(
      SortedSet<OSHDBTimestamp> timestamps,
      @Nonnull P boundingPolygon,
      TagInterpreter tagInterpreter,
      OSHEntityFilter oshEntityPreFilter,
      OSMEntityFilter osmEntityFilter,
      boolean includeOldStyleMultipolygons
  ) {
    this(
        timestamps,
        OSHDBGeometryBuilder.boundingBoxOf(boundingPolygon.getEnvelopeInternal()),
        boundingPolygon,
        tagInterpreter,
        oshEntityPreFilter,
        osmEntityFilter,
        includeOldStyleMultipolygons
    );
  }

  /**
   * Creates a cell iterator from a bounding box.
   *
   * @param timestamps a list of timestamps to return data for
   * @param tagInterpreter helper object which is used to determine entitie's geometry types
   * @param boundingBox only entities inside or intersecting this bbox are returned, geometries are
   *        clipped to this extent
   * @param oshEntityPreFilter (optional) a lambda called for each osh entity to pre-filter
   *        elements. only if it returns true, its osmEntity objects can be included in the output
   * @param osmEntityFilter a lambda called for each entity. if it returns true, the particular
   *        feature is included in the output
   * @param includeOldStyleMultipolygons if true, output contains also data for "old style
   *        multipolygons".
   *        Note, that if includeOldStyleMultipolygons is true, for each old style multipolygon only
   *        the geometry of the inner holes are returned (while the outer part is already present as
   *        the respective way's output)! This has to be interpreted separately and differently in
   *        the data analysis! The includeOldStyleMultipolygons is also quite a bit less efficient
   *        (both CPU and memory) as the default path.
   */
  public CellIterator(
      SortedSet<OSHDBTimestamp> timestamps,
      OSHDBBoundingBox boundingBox,
      TagInterpreter tagInterpreter,
      OSHEntityFilter oshEntityPreFilter,
      OSMEntityFilter osmEntityFilter,
      boolean includeOldStyleMultipolygons
  ) {
    this.timestamps = new TreeSet<>(timestamps);
    this.boundingBox = boundingBox;
    this.isBoundByPolygon = false; // todo: maybe replace this with a "dummy" polygonClipper?
    this.bboxInPolygon = null;
    this.bboxOutsidePolygon = null;
    this.fastPolygonClipper = null;
    this.tagInterpreter = tagInterpreter;
    this.oshEntityPreFilter = oshEntityPreFilter;
    this.osmEntityFilter = osmEntityFilter;
    this.includeOldStyleMultipolygons = includeOldStyleMultipolygons;
  }

  /**
   * Holds the result of a single item returned by {@link #iterateByTimestamps(GridOSHEntity)}.
   */
  public static class IterateByTimestampEntry {
    public final OSHDBTimestamp timestamp;
    public final OSMEntity osmEntity;
    public final OSHEntity oshEntity;
    public final LazyEvaluatedObject<Geometry> geometry;
    public final LazyEvaluatedObject<Geometry> unclippedGeometry;

    /**
     * Properties associated with each entity snapshot.
     *
     * @param timestamp timestamp of the snapshot
     * @param osmEntity the exact version of the OSM object
     * @param oshEntity the whole version history of the OSM object
     * @param geom an object which holds the geometry of the OSM object, or a method to build it
     *             on request, clipped to the query area of interest
     * @param unclippedGeom holds the full unclipped geometry of the OSM object
     *                     or a function to build it
     */
    public IterateByTimestampEntry(
        OSHDBTimestamp timestamp, @Nonnull OSMEntity osmEntity, @Nonnull OSHEntity oshEntity,
        LazyEvaluatedObject<Geometry> geom, LazyEvaluatedObject<Geometry> unclippedGeom
    ) {
      this.timestamp = timestamp;
      this.osmEntity = osmEntity;
      this.oshEntity = oshEntity;
      this.geometry = geom;
      this.unclippedGeometry = unclippedGeom;
    }
  }

  /**
   * Helper method to easily iterate over all entities in a cell that match a given condition/filter
   * as they existed at the given timestamps.
   *
   * @param cell the data cell
   *
   * @return a stream of matching filtered OSMEntities with their clipped Geometries at each
   *         timestamp. If an object has not been modified between timestamps, the output may
   *         contain the *same* Geometry object in the output multiple times. This can be used to
   *         optimize away recalculating expensive geometry operations on unchanged feature
   *         geometries later on in the code.
   */
  public Stream<IterateByTimestampEntry> iterateByTimestamps(GridOSHEntity cell) {
    final boolean allFullyInside;
    if (isBoundByPolygon) {
      // if cell is fully inside bounding box/polygon we can skip all entity-based inclusion checks
      OSHDBBoundingBox cellBoundingBox = XYGrid.getBoundingBox(new CellId(
          cell.getLevel(),
          cell.getId()
      ), true);
      if (bboxOutsidePolygon.test(cellBoundingBox)) {
        return Stream.empty();
      }
      allFullyInside = bboxInPolygon.test(cellBoundingBox);
    } else {
      allFullyInside = false;
    }

    Iterable<? extends OSHEntity> cellData = cell.getEntities();
    return Streams.stream(cellData).flatMap(oshEntity -> {
      if (!oshEntityPreFilter.test(oshEntity)
          || !allFullyInside && (
              !oshEntity.intersects(boundingBox)
              || (isBoundByPolygon && bboxOutsidePolygon.test(oshEntity))
          )) {
        // this osh entity doesn't match the prefilter or is fully outside the requested
        // area of interest -> skip it
        return Stream.empty();
      }
      if (Streams.stream(oshEntity.getVersions()).noneMatch(osmEntityFilter)) {
        // none of this osh entity's versions matches the filter -> skip it
        return Stream.empty();
      }
      boolean fullyInside = allFullyInside || (
          oshEntity.coveredBy(boundingBox)
          && (!isBoundByPolygon || bboxInPolygon.test(oshEntity))
      );

      // optimize loop by requesting modification timestamps first, and skip geometry calculations
      // where not needed
      SortedMap<OSHDBTimestamp, List<OSHDBTimestamp>> queryTs = new TreeMap<>();
      if (!includeOldStyleMultipolygons) {
        List<OSHDBTimestamp> modTs =
            OSHEntityTimeUtils.getModificationTimestamps(oshEntity, osmEntityFilter);
        int j = 0;
        for (OSHDBTimestamp requestedT : timestamps) {
          boolean needToRequest = false;
          while (j < modTs.size()
              && modTs.get(j).getEpochSecond() <= requestedT.getEpochSecond()) {
            needToRequest = true;
            j++;
          }
          if (needToRequest) {
            queryTs.put(requestedT, new LinkedList<>());
          } else if (queryTs.size() > 0) {
            queryTs.get(queryTs.lastKey()).add(requestedT);
          }
        }
      } else {
        // todo: make this work with old style multipolygons!!?!
        for (OSHDBTimestamp ts : timestamps) {
          queryTs.put(ts, new LinkedList<>());
        }
      }

      List<OSHDBTimestamp> timestamps = new ArrayList<>(queryTs.keySet());
      List<OSMEntity> osmEntityAtTimestamps = getVersionsByTimestamps(oshEntity, timestamps);

      List<IterateByTimestampEntry> results = new LinkedList<>();
      osmEntityLoop: for (int j = 0; j < osmEntityAtTimestamps.size(); j++) {
        OSHDBTimestamp timestamp = timestamps.get(j);
        OSMEntity osmEntity = osmEntityAtTimestamps.get(j);

        if (!osmEntity.isVisible()) {
          // skip because this entity is deleted at this timestamp
          continue;
        }
        if (osmEntity instanceof OSMWay && ((OSMWay) osmEntity).getMembers().length == 0
            || osmEntity instanceof OSMRelation
                && ((OSMRelation) osmEntity).getMembers().length == 0) {
          // skip way/relation with zero nodes/members
          continue;
        }

        boolean isOldStyleMultipolygon = false;
        if (includeOldStyleMultipolygons && osmEntity instanceof OSMRelation
            && tagInterpreter.isOldStyleMultipolygon((OSMRelation) osmEntity)) {
          final OSMRelation rel = (OSMRelation) osmEntity;
          for (int i = 0; i < rel.getMembers().length; i++) {
            final OSMMember relMember = rel.getMembers()[i];
            if (relMember.getType() == OSMType.WAY
                && tagInterpreter.isMultipolygonOuterMember(relMember)) {
              OSMEntity way = OSHEntities.getByTimestamp(relMember.getEntity(), timestamp);
              if (!osmEntityFilter.test(way)) {
                // skip this old-style-multipolygon because it doesn't match our filter
                continue osmEntityLoop;
              } else {
                // we know this multipolygon only has exactly one outer way, so we can abort the
                // loop and actually
                // "continue" with the calculations ^-^
                isOldStyleMultipolygon = true;
                break;
              }
            }
          }
        } else {
          if (!osmEntityFilter.test(osmEntity)) {
            // skip because this entity doesn't match our filter
            continue osmEntityLoop;
          }
        }

        try {
          LazyEvaluatedObject<Geometry> geom;
          if (!isOldStyleMultipolygon) {
            geom = constructClippedGeometry(osmEntity, timestamp, fullyInside);
          } else {
            // old style multipolygons: return only the inner holes of the geometry -> this is then
            // used to "fix" the results obtained from calculating the geometry on the object's
            // outer way which doesn't know about the inner members of the multipolygon relation
            // todo: check if this is all valid?
            GeometryFactory gf = new GeometryFactory();
            geom = new LazyEvaluatedObject<>(() -> {
              Geometry geometry = OSHDBGeometryBuilder
                  .getGeometry(osmEntity, timestamp, tagInterpreter);

              Polygon poly = (Polygon) geometry;
              Polygon[] interiorRings = new Polygon[poly.getNumInteriorRing()];
              for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                interiorRings[i] =
                    new Polygon((LinearRing) poly.getInteriorRingN(i), new LinearRing[]{}, gf);
              }
              geometry = new MultiPolygon(interiorRings, gf);
              if (!fullyInside) {
                geometry = isBoundByPolygon
                    ? fastPolygonClipper.intersection(geometry)
                    : Geo.clip(geometry, boundingBox);
              }
              return geometry;
            });
          }

          if (fullyInside || !geom.get().isEmpty()) {
            LazyEvaluatedObject<Geometry> fullGeom = fullyInside ? geom : new LazyEvaluatedObject<>(
                () -> OSHDBGeometryBuilder.getGeometry(osmEntity, timestamp, tagInterpreter));
            results.add(
                new IterateByTimestampEntry(timestamp, osmEntity, oshEntity, geom, fullGeom)
            );
            // add skipped timestamps (where nothing has changed from the last timestamp) to result
            for (OSHDBTimestamp additionalT : queryTs.get(timestamp)) {
              results.add(
                  new IterateByTimestampEntry(additionalT, osmEntity, oshEntity, geom, fullGeom)
              );
            }
          }
        } catch (IllegalArgumentException err) {
          // maybe some corner case where JTS doesn't support operations on a broken geometry
          LOG.info("Entity {}/{} skipped because of invalid geometry at timestamp {}",
              osmEntity.getType().toString().toLowerCase(), osmEntity.getId(), timestamp);
        } catch (TopologyException err) {
          // happens e.g. in JTS intersection method when geometries are self-overlapping
          LOG.info("Topology error with entity {}/{} at timestamp {}: {}",
              osmEntity.getType().toString().toLowerCase(), osmEntity.getId(), timestamp,
              err.toString());
        }
      }
      // stream this oshEntity's results
      return results.stream();
    });
  }

  private LazyEvaluatedObject<Geometry> constructClippedGeometry(
      OSMEntity osmEntity,
      OSHDBTimestamp timestamp,
      boolean fullyInside
  ) {
    if (fullyInside) {
      return new LazyEvaluatedObject<>(() ->
          OSHDBGeometryBuilder.getGeometry(osmEntity, timestamp, tagInterpreter)
      );
    }
    Geometry geometry = OSHDBGeometryBuilder.getGeometry(osmEntity, timestamp, tagInterpreter);
    OSHDBBoundingBox bbox = OSHDBGeometryBuilder.boundingBoxOf(geometry.getEnvelopeInternal());
    if (isBoundByPolygon) {
      if (bboxInPolygon.test(bbox)) {
        return new LazyEvaluatedObject<>(geometry);
      } else if (bboxOutsidePolygon.test(bbox)) {
        return new LazyEvaluatedObject<>(createEmptyGeometryLike(geometry));
      } else {
        return new LazyEvaluatedObject<>(fastPolygonClipper.intersection(geometry));
      }
    } else {
      if (bbox.coveredBy(this.boundingBox)) {
        return new LazyEvaluatedObject<>(geometry);
      } else if (!bbox.intersects(this.boundingBox)) {
        return new LazyEvaluatedObject<>(createEmptyGeometryLike(geometry));
      } else {
        return new LazyEvaluatedObject<>(Geo.clip(geometry, this.boundingBox));
      }
    }
  }

  private Geometry createEmptyGeometryLike(Geometry geometry) {
    GeometryFactory gf = new GeometryFactory();
    if (geometry instanceof Polygonal) {
      return gf.createPolygon((LinearRing) null);
    } else if (geometry instanceof Lineal) {
      return gf.createLineString((CoordinateSequence) null);
    } else if (geometry instanceof Puntal) {
      return gf.createPoint((Coordinate) null);
    } else {
      return gf.createGeometryCollection(null);
    }
  }

  /**
   * Holds the result of a single item returned by {@link #iterateByContribution(GridOSHEntity)}.
   */
  public static class IterateAllEntry {
    public final OSHDBTimestamp timestamp;
    @Nonnull
    public final OSMEntity osmEntity;
    public final OSMEntity previousOsmEntity;
    public final OSHEntity oshEntity;
    public final LazyEvaluatedObject<Geometry> geometry;
    public final LazyEvaluatedObject<Geometry> previousGeometry;
    public final LazyEvaluatedObject<Geometry> unclippedGeometry;
    public final LazyEvaluatedObject<Geometry> unclippedPreviousGeometry;
    public final LazyEvaluatedContributionTypes activities;
    public final long changeset;

    /**
     * Properties associated with each contribution object.
     *
     * @param timestamp the timestamp when the OSM object was modified
     * @param osmEntity the version of the OSM object after the modification
     * @param previousOsmEntity the version of the OSM object before the modification
     * @param oshEntity the full version history of the OSM object
     * @param geometry the geometry of the OSM object (or a function to build it) of the state
     *                 of the OSM object after the modification, clipped to the query
     *                 area of interest
     * @param previousGeometry the geometry of the OSM object (or a function to build it) of
     *                         the state of the OSM object before the modification, clipped to the
     *                         the query area of interest
     * @param unclippedGeometry same as {@link #geometry}, but not clipped to the query area
     * @param previousUnclippedGeometry same as {@link #previousGeometry}, but not clipped to
     *                                  the query area
     * @param activities a set of contribution types this modification can be classified with
     * @param changeset the changeset id this data modification is a part of
     */
    public IterateAllEntry(
        OSHDBTimestamp timestamp,
        @Nonnull OSMEntity osmEntity, OSMEntity previousOsmEntity, @Nonnull OSHEntity oshEntity,
        LazyEvaluatedObject<Geometry> geometry, LazyEvaluatedObject<Geometry> previousGeometry,
        LazyEvaluatedObject<Geometry> unclippedGeometry,
        LazyEvaluatedObject<Geometry> previousUnclippedGeometry,
        LazyEvaluatedContributionTypes activities,
        long changeset
    ) {
      this.timestamp = timestamp;
      this.osmEntity = osmEntity;
      this.previousOsmEntity = previousOsmEntity;
      this.oshEntity = oshEntity;
      this.geometry = geometry;
      this.previousGeometry = previousGeometry;
      this.unclippedGeometry = unclippedGeometry;
      this.unclippedPreviousGeometry = previousUnclippedGeometry;
      this.activities = activities;
      this.changeset = changeset;
    }
  }

  /**
   * Helper method to easily iterate over all entity modifications in a cell that match a given
   * condition/filter.
   *
   * @param cell the data cell
   *
   * @return a stream of matching filtered OSMEntities with their clipped Geometries and timestamp
   *         intervals.
   */
  public Stream<IterateAllEntry> iterateByContribution(GridOSHEntity cell) {
    OSHDBTimestampInterval timeInterval = new OSHDBTimestampInterval(timestamps);

    final boolean allFullyInside;
    if (isBoundByPolygon) {
      // if cell is fully inside bounding box/polygon we can skip all entity-based inclusion checks
      OSHDBBoundingBox cellBoundingBox = XYGrid.getBoundingBox(new CellId(
          cell.getLevel(),
          cell.getId()
      ), true);
      if (bboxOutsidePolygon.test(cellBoundingBox)) {
        return Stream.empty();
      }
      allFullyInside = bboxInPolygon.test(cellBoundingBox);
    } else {
      allFullyInside = false;
    }

    if (includeOldStyleMultipolygons) {
      //todo: remove this by finishing the functionality below
      throw new UnsupportedOperationException("this is not yet properly implemented (probably)");
    }

    Iterable<? extends OSHEntity> cellData = cell.getEntities();

    return Streams.stream(cellData).flatMap(oshEntity -> {
      if (!oshEntityPreFilter.test(oshEntity)
          || !allFullyInside && (
              !oshEntity.intersects(boundingBox)
              || (isBoundByPolygon && bboxOutsidePolygon.test(oshEntity))
          )) {
        // this osh entity doesn't match the prefilter or is fully outside the requested
        // area of interest -> skip it
        return Stream.empty();
      }
      if (Streams.stream(oshEntity.getVersions()).noneMatch(osmEntityFilter)) {
        // none of this osh entity's versions matches the filter -> skip it
        return Stream.empty();
      }

      boolean fullyInside = allFullyInside || (
          oshEntity.coveredBy(boundingBox)
          && (!isBoundByPolygon || bboxInPolygon.test(oshEntity))
      );

      Map<OSHDBTimestamp, Long> changesetTs = OSHEntityTimeUtils.getChangesetTimestamps(oshEntity);
      List<OSHDBTimestamp> modTs =
          OSHEntityTimeUtils.getModificationTimestamps(oshEntity, osmEntityFilter, changesetTs);

      if (modTs.isEmpty() || !timeInterval.intersects(
          new OSHDBTimestampInterval(modTs.get(0), modTs.get(modTs.size() - 1))
      )) {
        // ignore osh entity because it's edit history is fully outside of the given time interval
        // of interest
        return Stream.empty();
      }

      List<OSMEntity> osmEntityAtTimestamps = getVersionsByTimestamps(oshEntity, modTs);

      List<IterateAllEntry> results = new LinkedList<>();

      IterateAllEntry prev = null;

      osmEntityLoop:
      for (int j = 0; j < osmEntityAtTimestamps.size(); j++) {
        OSHDBTimestamp timestamp = modTs.get(j);
        OSMEntity osmEntity = osmEntityAtTimestamps.get(j);

        // prev = results.size() > 0 ? results.get(results.size()-1) : null;
        // todo: replace with variable outside of osmEntitiyLoop (than we can also get rid of
        // the `|| prev.osmEntity.getId() != osmEntity.getId()`'s below)
        boolean skipOutput = false;

        OSHDBTimestamp nextTs = null;
        // todo: better way to figure out if timestamp is not last element??
        if (modTs.size() > modTs.indexOf(timestamp) + 1) {
          nextTs = modTs.get(modTs.indexOf(timestamp) + 1);
        }

        if (!timeInterval.includes(timestamp)) {
          // ignore osm entity because it's outside of the given time interval of interest
          if (timeInterval.compareTo(timestamp) > 0) { // timestamp in the future of the interval
            break; // abort current osmEntityByTimestamps loop, continue with next osh entity
          } else if (!timeInterval.includes(nextTs)) { // next modification state is also in not in
            // our time frame of interest
            continue; // continue with next mod. state of current osh entity
          } else {
            // next mod. state of current entity will be in the time range of interest. -> skip it
            // but we still have to process this entity fully, because we need stuff in `prev` for
            // previousGeometry, etc. during the next iteration
            skipOutput = true;
          }
        }

        if (!osmEntity.isVisible()) {
          // this entity is deleted at this timestamp
          // todo: some of this may be refactorable between the two for loops
          if (prev != null && !prev.activities.contains(ContributionType.DELETION)) {
            prev = new IterateAllEntry(timestamp,
                osmEntity, prev.osmEntity, oshEntity,
                new LazyEvaluatedObject<>((Geometry) null), prev.geometry,
                new LazyEvaluatedObject<>((Geometry) null), prev.unclippedGeometry,
                new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.DELETION)),
                osmEntity.getChangesetId()
            );
            // cannot normally happen, because prev is never null while skipOutput is true (since no
            // previous result has yet been generated before the first modification in the query
            // timestamp inteval). But if the oshdb-api would at some point have to support non-
            // contiguous timestamp intervals, this case could be needed.
            if (!skipOutput) {
              results.add(prev);
            }
          }
          continue osmEntityLoop;
        }

        // todo check old style mp code!!1!!!11!
        boolean isOldStyleMultipolygon = false;
        if (includeOldStyleMultipolygons && osmEntity instanceof OSMRelation
            && tagInterpreter.isOldStyleMultipolygon((OSMRelation) osmEntity)) {
          final OSMRelation rel = (OSMRelation) osmEntity;
          for (int i = 0; i < rel.getMembers().length; i++) {
            final OSMMember relMember = rel.getMembers()[i];
            if (relMember.getType() == OSMType.WAY
                && tagInterpreter.isMultipolygonOuterMember(relMember)) {
              OSMEntity way = OSHEntities.getByTimestamp(relMember.getEntity(), timestamp);
              if (!osmEntityFilter.test(way)) {
                // skip this old-style-multipolygon because it doesn't match our filter
                continue osmEntityLoop;
              } else {
                // we know this multipolygon only has exactly one outer way, so we can abort the
                // loop and actually
                // "continue" with the calculations ^-^
                isOldStyleMultipolygon = true;
                break;
              }
            }
          }
        } else {
          if (!osmEntityFilter.test(osmEntity)) {
            // this entity doesn't match our filter (anymore)
            // TODO?: separate/additional activity type (e.g. "RECYCLED" ??) and still construct
            // geometries for these?
            if (prev != null && !prev.activities.contains(ContributionType.DELETION)) {
              prev = new IterateAllEntry(timestamp,
                  osmEntity, prev.osmEntity, oshEntity,
                  new LazyEvaluatedObject<>((Geometry) null), prev.geometry,
                  new LazyEvaluatedObject<>((Geometry) null), prev.unclippedGeometry,
                  new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.DELETION)),
                  changesetTs.get(timestamp)
              );
              if (!skipOutput) {
                results.add(prev);
              }
            }
            continue osmEntityLoop;
          }
        }

        try {
          LazyEvaluatedObject<Geometry> geom;
          if (!isOldStyleMultipolygon) {
            geom = constructClippedGeometry(osmEntity, timestamp, fullyInside);
          } else {
            // old style multipolygons: return only the inner holes of the geometry -> this is then
            // used to "fix" the results obtained from calculating the geometry on the object's
            // outer way which doesn't know about the inner members of the multipolygon relation
            // todo: check if this is all valid?
            GeometryFactory gf = new GeometryFactory();
            geom = new LazyEvaluatedObject<>(() -> {
              Geometry geometry =
                  OSHDBGeometryBuilder.getGeometry(osmEntity, timestamp, tagInterpreter);
              Polygon poly = (Polygon) geometry;
              Polygon[] interiorRings = new Polygon[poly.getNumInteriorRing()];
              for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                interiorRings[i] =
                    new Polygon((LinearRing) poly.getInteriorRingN(i), new LinearRing[]{}, gf);
              }
              geometry = new MultiPolygon(interiorRings, gf);
              if (!fullyInside) {
                geometry = Geo.clip(geometry, boundingBox);
              }
              return geometry;
            });
          }

          LazyEvaluatedContributionTypes activity;
          if (!fullyInside && geom.get().isEmpty()) {
            // either object is outside of current area or has invalid geometry
            if (prev != null && !prev.activities.contains(ContributionType.DELETION)) {
              prev = new IterateAllEntry(timestamp,
                  osmEntity, prev.osmEntity, oshEntity,
                  new LazyEvaluatedObject<>((Geometry) null), prev.geometry,
                  new LazyEvaluatedObject<>((Geometry) null), prev.unclippedGeometry,
                  new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.DELETION)),
                  changesetTs.get(timestamp)
              );
              if (!skipOutput) {
                results.add(prev);
              }
            }
            continue osmEntityLoop;
          } else if (prev == null || prev.activities.contains(ContributionType.DELETION)) {
            activity = new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.CREATION));
            // todo: special case when an object gets specific tag/condition again after having
            // them removed?
          } else {
            OSMEntity prevEntity = prev.osmEntity;
            LazyEvaluatedObject<Geometry> prevGeometry = prev.geometry;
            activity = new LazyEvaluatedContributionTypes(contributionType -> {
              switch (contributionType) {
                case TAG_CHANGE:
                  // look if tags have been changed between versions
                  boolean tagsChange = false;
                  if (prevEntity.getRawTags().length != osmEntity.getRawTags().length) {
                    tagsChange = true;
                  } else {
                    for (int i = 0; i < prevEntity.getRawTags().length; i++) {
                      if (prevEntity.getRawTags()[i] != osmEntity.getRawTags()[i]) {
                        tagsChange = true;
                        break;
                      }
                    }
                  }
                  return tagsChange;
                case GEOMETRY_CHANGE:
                  // look if geometry has been changed between versions
                  return !prevGeometry.equals(geom);
                default:
                  return false;
              }
            });
          }

          IterateAllEntry result;
          LazyEvaluatedObject<Geometry> unclippedGeom = new LazyEvaluatedObject<>(() ->
              OSHDBGeometryBuilder.getGeometry(osmEntity, timestamp, tagInterpreter)
          );
          if (prev != null) {
            result = new IterateAllEntry(timestamp,
                osmEntity, prev.osmEntity, oshEntity,
                geom, prev.geometry,
                unclippedGeom, prev.unclippedGeometry,
                activity,
                changesetTs.get(timestamp)
            );
          } else {
            result = new IterateAllEntry(timestamp,
                osmEntity, null, oshEntity,
                geom, new LazyEvaluatedObject<>((Geometry) null),
                unclippedGeom, new LazyEvaluatedObject<>((Geometry) null),
                activity,
                changesetTs.get(timestamp)
            );
          }

          if (!skipOutput) {
            results.add(result);
          }
          prev = result;
        } catch (IllegalArgumentException err) {
          // maybe some corner case where JTS doesn't support operations on a broken geometry
          LOG.info("Entity {}/{} skipped because of invalid geometry at timestamp {}",
              osmEntity.getType().toString().toLowerCase(), osmEntity.getId(), timestamp);
        } catch (TopologyException err) {
          // happens e.g. in JTS intersection method when geometries are self-overlapping
          LOG.info("Topology error with entity {}/{} at timestamp {}: {}",
              osmEntity.getType().toString().toLowerCase(), osmEntity.getId(), timestamp,
              err.toString());
        }
      }
      // stream this oshEntity's results
      return results.stream();
    });
  }

  /**
   * Returns a list of corresponding osm OSMEntity "versions" of the given OSHEntity which are
   * valid at the given timestamps. The resulting list will contain the same number of entries
   * as the supplied list of timestamps.
   */
  private static List<OSMEntity> getVersionsByTimestamps(
      OSHEntity osh, List<OSHDBTimestamp> timestamps) {
    List<OSMEntity> result = new ArrayList<>(timestamps.size());

    int i = timestamps.size() - 1;
    Iterator<? extends OSMEntity> itr = osh.getVersions().iterator();
    while (itr.hasNext() && i >= 0) {
      OSMEntity osm = itr.next();
      while (i >= 0 && osm.getTimestamp().compareTo(timestamps.get(i)) <= 0) {
        result.add(osm);
        i--;
      }
    }
    return Lists.reverse(result);
  }
}