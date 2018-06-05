package org.heigit.bigspatialdata.oshdb.util.celliterator;

import com.vividsolutions.jts.geom.*;
import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.*;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastBboxInPolygon;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastBboxOutsidePolygon;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastPolygonOperations;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CellIterator implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(CellIterator.class);

  public interface OSHEntityFilter extends Predicate<OSHEntity>, Serializable {};
  public interface OSMEntityFilter extends Predicate<OSMEntity>, Serializable {};

  private TreeSet<OSHDBTimestamp> timestamps;
  private OSHDBBoundingBox boundingBox;
  private boolean isBoundByPolygon;
  private FastBboxInPolygon bboxInPolygon;
  private FastBboxOutsidePolygon bboxOutsidePolygon;
  private FastPolygonOperations fastPolygonClipper;
  private TagInterpreter tagInterpreter;
  private OSHEntityFilter oshEntityPreFilter;
  private OSMEntityFilter osmEntityFilter;
  private boolean includeOldStyleMultipolygons;

  /**
   * todo…
   *
   * @param timestamps a list of timestamps to return data for
   * @param boundingBox only entities inside or intersecting this bbox are returned, geometries are
   *        clipped to this extent
   * @param boundingPolygon only entities inside or intersecting this polygon are returned,
   *        geometries are clipped to this extent. If present, the supplied boundingBox must be the
   *        boundingBox of this polygon
   * @param oshEntityPreFilter (optional) a lambda called for each osh entity to pre-filter
   *        elements. only if it returns true, its osmEntity objects can be included in the output
   * @param osmEntityFilter a lambda called for each entity. if it returns true, the particular
   *        feature is included in the output
   * @param includeOldStyleMultipolygons if true, output contains also data for "old style
   *        multipolygons".
   *
   *        Note, that if includeOldStyleMultipolygons is true, for each old style multipolygon only
   *        the geometry of the inner holes are returned (while the outer part is already present as
   *        the respective way's output)! This has to be interpreted separately and differently in
   *        the data analysis! The includeOldStyleMultipolygons is also quite a bit less efficient
   *        (both CPU and memory) as the default path.
   */
  public <P extends Geometry & Polygonal> CellIterator(
      SortedSet<OSHDBTimestamp> timestamps,
      OSHDBBoundingBox boundingBox,
      P boundingPolygon,
      TagInterpreter tagInterpreter,
      OSHEntityFilter oshEntityPreFilter, OSMEntityFilter osmEntityFilter,
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
    this.isBoundByPolygon = false; // todo: is this flag even needed? -> replace by "dummy" polygonClipper?
    this.bboxInPolygon = null;
    this.bboxOutsidePolygon = null;
    this.fastPolygonClipper = null;
    this.tagInterpreter = tagInterpreter;
    this.oshEntityPreFilter = oshEntityPreFilter;
    this.osmEntityFilter = osmEntityFilter;
    this.includeOldStyleMultipolygons = includeOldStyleMultipolygons;
  }

  public static class IterateByTimestampEntry {
    public final OSHDBTimestamp timestamp;
    public final OSMEntity osmEntity;
    public final OSHEntity oshEntity;
    public final LazyEvaluatedObject<Geometry> geometry;
    public final LazyEvaluatedObject<Geometry> unclippedGeometry;

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
    List<IterateByTimestampEntry> results = new LinkedList<>();

    boolean allFullyInside = false;
    if (isBoundByPolygon) {
      // if cell is fully inside bounding box/polygon we can skip all entity-based inclusion checks
      OSHDBBoundingBox cellBoundingBox = XYGrid.getBoundingBox(new CellId(
          cell.getLevel(),
          cell.getId()
      ));
      if (bboxOutsidePolygon.test(cellBoundingBox)) {
        return results.stream();
      }
      allFullyInside = bboxInPolygon.test(cellBoundingBox);
    }

    for (OSHEntity<OSMEntity> oshEntity : (Iterable<OSHEntity<OSMEntity>>) cell) {
      if (!oshEntityPreFilter.test(oshEntity) ||
          !allFullyInside && (
              !oshEntity.getBoundingBox().intersects(boundingBox) ||
              (isBoundByPolygon && bboxOutsidePolygon.test(oshEntity.getBoundingBox()))
      )) {
        // this osh entity doesn't match the prefilter or is fully outside the requested
        // area of interest -> skip it
        continue;
      }
      boolean fullyInside = allFullyInside || (
          oshEntity.getBoundingBox().isInside(boundingBox) &&
          (!isBoundByPolygon || bboxInPolygon.test(boundingBox))
      );

      // optimize loop by requesting modification timestamps first, and skip geometry calculations
      // where not needed
      SortedMap<OSHDBTimestamp, List<OSHDBTimestamp>> queryTs = new TreeMap<>();
      if (!includeOldStyleMultipolygons) {
        List<OSHDBTimestamp> modTs = oshEntity.getModificationTimestamps(osmEntityFilter);
        int j = 0;
        for (OSHDBTimestamp requestedT : timestamps) {
          boolean needToRequest = false;
          while (j < modTs.size() && modTs.get(j).getRawUnixTimestamp() <= requestedT.getRawUnixTimestamp()) {
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

      SortedMap<OSHDBTimestamp, OSMEntity> osmEntityByTimestamps =
          oshEntity.getByTimestamps(new ArrayList<>(queryTs.keySet()));

      osmEntityLoop: for (Map.Entry<OSHDBTimestamp, OSMEntity> entity : osmEntityByTimestamps.entrySet()) {
        OSHDBTimestamp timestamp = entity.getKey();
        OSMEntity osmEntity = entity.getValue();

        if (!osmEntity.isVisible()) {
          // skip because this entity is deleted at this timestamp
          continue;
        }
        if (osmEntity instanceof OSMWay && ((OSMWay)osmEntity).getRefs().length == 0 ||
            osmEntity instanceof OSMRelation && ((OSMRelation)osmEntity).getMembers().length == 0) {
          // skip way/relation with zero nodes/members
          continue;
        }

        boolean isOldStyleMultipolygon = false;
        if (includeOldStyleMultipolygons && osmEntity instanceof OSMRelation
            && tagInterpreter.isOldStyleMultipolygon((OSMRelation) osmEntity)) {
          OSMRelation rel = (OSMRelation) osmEntity;
          for (int i = 0; i < rel.getMembers().length; i++) {
            if (rel.getMembers()[i].getType() == OSMType.WAY
                && tagInterpreter.isMultipolygonOuterMember(rel.getMembers()[i])) {
              OSMEntity way = rel.getMembers()[i].getEntity().getByTimestamp(timestamp);
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
            // used to "fix" the
            // results obtained from calculating the geometry on the object's outer way which
            // doesn't know about the
            // inner members of the multipolygon relation
            // todo: check if this is all valid?
            GeometryFactory gf = new GeometryFactory();
            geom = new LazyEvaluatedObject<>(() -> {
              Geometry _geom = OSHDBGeometryBuilder
                  .getGeometry(osmEntity, timestamp, tagInterpreter);

              Polygon poly = (Polygon) _geom;
              Polygon[] interiorRings = new Polygon[poly.getNumInteriorRing()];
              for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                interiorRings[i] =
                    new Polygon((LinearRing) poly.getInteriorRingN(i), new LinearRing[]{}, gf);
              }
              _geom = new MultiPolygon(interiorRings, gf);
              if (!fullyInside) {
                _geom = isBoundByPolygon
                    ? fastPolygonClipper.intersection(_geom)
                    : Geo.clip(_geom, boundingBox);
              }
              return _geom;
            });
          }

          if (fullyInside || (geom.get() != null && !geom.get().isEmpty())) {
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
        } catch (UnsupportedOperationException err) {
          // e.g. unsupported relation types go here
        } catch (IllegalArgumentException err) {
          LOG.info("Entity {}/{} skipped because of invalid geometry at timestamp {}",
              osmEntity.getType().toString().toLowerCase(), osmEntity.getId(), timestamp);
        } catch (TopologyException err) {
          // todo: can this even happen?
          LOG.info("Topology error with entity {}/{} at timestamp {}: {}",
              osmEntity.getType().toString().toLowerCase(), osmEntity.getId(), timestamp,
              err.toString());
        }
      }
    }

    // return as an obj stream
    return results.stream();
  }

  private LazyEvaluatedObject<Geometry> constructClippedGeometry(OSMEntity osmEntity,
      OSHDBTimestamp timestamp, boolean fullyInside) {
    LazyEvaluatedObject<Geometry> geom;
    if (fullyInside) {
      geom = new LazyEvaluatedObject<>(() ->
          OSHDBGeometryBuilder.getGeometry(osmEntity, timestamp, tagInterpreter)
      );
    } else if (isBoundByPolygon) {
      geom = new LazyEvaluatedObject<>(fastPolygonClipper.intersection(
          OSHDBGeometryBuilder.getGeometry(osmEntity, timestamp, tagInterpreter)
      ));
    } else {
      geom = new LazyEvaluatedObject<>(
          OSHDBGeometryBuilder.getGeometryClipped(
              osmEntity, timestamp, tagInterpreter, boundingBox
          )
      );
    }
    return geom;
  }

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
    public long changeset;

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
    List<IterateAllEntry> results = new LinkedList<>();

    boolean allFullyInside = false;
    if (isBoundByPolygon) {
      // if cell is fully inside bounding box/polygon we can skip all entity-based inclusion checks
      OSHDBBoundingBox cellBoundingBox = XYGrid.getBoundingBox(new CellId(
          cell.getLevel(),
          cell.getId()
      ));
      if (bboxOutsidePolygon.test(cellBoundingBox)) {
        return results.stream();
      }
      allFullyInside = bboxInPolygon.test(cellBoundingBox);
    }

    if (includeOldStyleMultipolygons)
      throw new Error("this is not yet properly implemented (probably)"); //todo: remove this by finishing the functionality below

    for (OSHEntity<OSMEntity> oshEntity : (Iterable<OSHEntity<OSMEntity>>) cell) {
      if (!oshEntityPreFilter.test(oshEntity) ||
          !allFullyInside && (
              !oshEntity.getBoundingBox().intersects(boundingBox) ||
              (isBoundByPolygon && bboxOutsidePolygon.test(oshEntity.getBoundingBox()))
      )) {
        // this osh entity doesn't match the prefilter or is fully outside the requested
        // area of interest -> skip it
        continue;
      }

      boolean fullyInside = allFullyInside || (
          oshEntity.getBoundingBox().isInside(boundingBox) &&
          (!isBoundByPolygon || bboxInPolygon.test(boundingBox))
      );

      Map<OSHDBTimestamp, Long> changesetTs = oshEntity.getChangesetTimestamps();
      List<OSHDBTimestamp> modTs = oshEntity.getModificationTimestamps(osmEntityFilter, changesetTs);

      if (modTs.size() == 0 || !timeInterval
          .intersects(new OSHDBTimestampInterval(modTs.get(0), modTs.get(modTs.size() - 1)))) {
        continue; // ignore osh entity because it's edit history is fully outside of the given time
      } // interval of interest

      SortedMap<OSHDBTimestamp, OSMEntity> osmEntityByTimestamps = oshEntity.getByTimestamps(modTs);

      IterateAllEntry prev = null;
      osmEntityLoop: for (Map.Entry<OSHDBTimestamp, OSMEntity> entity : osmEntityByTimestamps.entrySet()) {
        OSHDBTimestamp timestamp = entity.getKey();
        OSMEntity osmEntity = entity.getValue();

        // prev = results.size() > 0 ? results.get(results.size()-1) : null;
        // todo: replace with variable outside of osmEntitiyLoop (than we can also get rid of the ` || prev.osmEntity.getId() != osmEntity.getId()`'s below)
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
                new LazyEvaluatedObject<>((Geometry)null), prev.geometry,
                new LazyEvaluatedObject<>((Geometry)null), prev.unclippedGeometry,
                new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.DELETION)),
                osmEntity.getChangeset()
            );
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
          OSMRelation rel = (OSMRelation) osmEntity;
          for (int i = 0; i < rel.getMembers().length; i++) {
            if (rel.getMembers()[i].getType() == OSMType.WAY
                && tagInterpreter.isMultipolygonOuterMember(rel.getMembers()[i])) {
              OSMEntity way = rel.getMembers()[i].getEntity().getByTimestamp(timestamp);
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
                  new LazyEvaluatedObject<>((Geometry)null), prev.geometry,
                  new LazyEvaluatedObject<>((Geometry)null), prev.unclippedGeometry,
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
            // used to "fix" the results obtained from calculating the geometry on the object's outer
            // way which doesn't know about the inner members of the multipolygon relation
            // todo: check if this is all valid?
            GeometryFactory gf = new GeometryFactory();
            geom = new LazyEvaluatedObject<>(() -> {
              Geometry _geom = OSHDBGeometryBuilder.getGeometry(osmEntity, timestamp, tagInterpreter);
              Polygon poly = (Polygon) _geom;
              Polygon[] interiorRings = new Polygon[poly.getNumInteriorRing()];
              for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                interiorRings[i] =
                    new Polygon((LinearRing) poly.getInteriorRingN(i), new LinearRing[]{}, gf);
              }
              _geom = new MultiPolygon(interiorRings, gf);
              if (!fullyInside) {
                _geom = Geo.clip(_geom, boundingBox);
              }
              return _geom;
            });
          }

          LazyEvaluatedContributionTypes activity;
          if (!fullyInside && (geom.get() == null || geom.get().isEmpty())) {
            // either object is outside of current area or has invalid geometry
            if (prev != null && !prev.activities.contains(ContributionType.DELETION)) {
              prev = new IterateAllEntry(timestamp,
                  osmEntity, prev.osmEntity, oshEntity,
                  new LazyEvaluatedObject<>((Geometry)null), prev.geometry,
                  new LazyEvaluatedObject<>((Geometry)null), prev.unclippedGeometry,
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
            // todo: special case when an object gets specific tag/condition again after having them
            // removed?
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
                  boolean geometryChange = false;
                  if (geom.get() != null && prevGeometry.get() != null) {
                    // todo: what if both are null? -> maybe fall back to MEMBER_CHANGE?
                    // todo: check: does this work as expected?
                    geometryChange = !prevGeometry.equals(geom);
                  }
                  return geometryChange;
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
                geom, new LazyEvaluatedObject<>((Geometry)null),
                unclippedGeom, new LazyEvaluatedObject<>((Geometry)null),
                activity,
                changesetTs.get(timestamp)
            );
          }

          if (!skipOutput) {
            results.add(result);
          }
          prev = result;
        } catch (UnsupportedOperationException err) {
          // e.g. unsupported relation types go here
        } catch (IllegalArgumentException err) {
          LOG.info("Entity {}/{} skipped because of invalid geometry at timestamp {}",
              osmEntity.getType().toString().toLowerCase(), osmEntity.getId(), timestamp);
        } catch (TopologyException err) {
          LOG.info("Topology error with entity {}/{} at timestamp {}: {}",
              osmEntity.getType().toString().toLowerCase(), osmEntity.getId(), timestamp,
              err.toString());
        }
      }
    }

    // return as an obj stream
    return results.stream();
  }

}


