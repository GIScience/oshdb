package org.heigit.bigspatialdata.oshdb.util.celliterator;

import com.vividsolutions.jts.geom.*;
import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CellIterator implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(CellIterator.class);

  private OSHDBBoundingBox boundingBox;
  private boolean isBoundByPolygon;
  private FastBboxInPolygon bboxInPolygon;
  private FastBboxOutsidePolygon bboxOutsidePolygon;
  private FastPolygonOperations fastPolygonClipper;
  private TagInterpreter tagInterpreter;
  private Predicate<OSHEntity> oshEntityPreFilter;
  private Predicate<OSMEntity> osmEntityFilter;
  private boolean includeOldStyleMultipolygons;

  /**
   * todo…
   *
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
  public <P extends Geometry & Polygonal> CellIterator(OSHDBBoundingBox boundingBox,
      P boundingPolygon,
      TagInterpreter tagInterpreter, Predicate<OSHEntity> oshEntityPreFilter,
      Predicate<OSMEntity> osmEntityFilter, boolean includeOldStyleMultipolygons) {
    this(
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
  public CellIterator(OSHDBBoundingBox boundingBox,
      TagInterpreter tagInterpreter, Predicate<OSHEntity> oshEntityPreFilter,
      Predicate<OSMEntity> osmEntityFilter, boolean includeOldStyleMultipolygons) {
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
    public final LazyEvaluatedObject<Geometry> geometry;

    IterateByTimestampEntry(
        OSHDBTimestamp timestamp, OSMEntity entity, LazyEvaluatedObject<Geometry> geom
    ) {
      this.timestamp = timestamp;
      this.osmEntity = entity;
      this.geometry = geom;
    }
  }

  /**
   * Helper method to easily iterate over all entities in a cell that match a given condition/filter
   * as they existed at the given timestamps.
   *
   * @param cell the data cell
   * @param timestamps a list of timestamps to return data for
   *
   * @return a stream of matching filtered OSMEntities with their clipped Geometries at each
   *         timestamp. If an object has not been modified between timestamps, the output may
   *         contain the *same* Geometry object in the output multiple times. This can be used to
   *         optimize away recalculating expensive geometry operations on unchanged feature
   *         geometries later on in the code.
   */
  public Stream<IterateByTimestampEntry> iterateByTimestamps(
      GridOSHEntity cell, SortedSet<OSHDBTimestamp> timestamps
  ) {
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
              !oshEntity.intersectsBbox(boundingBox) ||
              (isBoundByPolygon && bboxOutsidePolygon.test(oshEntity.getBoundingBox()))
      )) {
        // this osh entity doesn't match the prefilter or is fully outside the requested
        // area of interest -> skip it
        continue;
      }
      boolean fullyInside = allFullyInside || (
          oshEntity.insideBbox(boundingBox) &&
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
          while (j < modTs.size() && modTs.get(j).getRawUnixTimestamp() < requestedT.getRawUnixTimestamp()) {
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
            results.add(new IterateByTimestampEntry(timestamp, osmEntity, geom));
            // add skipped timestamps (where nothing has changed from the last timestamp) to set of
            // results
            for (OSHDBTimestamp additionalTimestamp : queryTs.get(timestamp)) {
              results.add(new IterateByTimestampEntry(additionalTimestamp, osmEntity, geom));
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

  public static class TimestampInterval {
    private OSHDBTimestamp fromTimestamp;
    private OSHDBTimestamp toTimestamp;

    public TimestampInterval() {
      this(new OSHDBTimestamp(Long.MIN_VALUE), new OSHDBTimestamp(Long.MAX_VALUE));
    }

    public TimestampInterval(OSHDBTimestamp fromTimestamp, OSHDBTimestamp toTimestamp) {
      this.fromTimestamp = fromTimestamp;
      this.toTimestamp = toTimestamp;
    }

    public TimestampInterval(SortedSet<OSHDBTimestamp> oshdbTimestamps) {
      this(oshdbTimestamps.first(), oshdbTimestamps.last());
    }

    public boolean intersects(TimestampInterval other) {
      return other.toTimestamp.getRawUnixTimestamp() >= this.fromTimestamp.getRawUnixTimestamp()
          && other.fromTimestamp.getRawUnixTimestamp() <= this.toTimestamp.getRawUnixTimestamp();
    }

    public boolean includes(OSHDBTimestamp timestamp) {
      return timestamp.getRawUnixTimestamp() >= this.fromTimestamp.getRawUnixTimestamp()
          && timestamp.getRawUnixTimestamp() < this.toTimestamp.getRawUnixTimestamp();
    }

    public int compareTo(OSHDBTimestamp timestamp) {
      if (this.includes(timestamp)) {
        return 0;
      }
      return timestamp.getRawUnixTimestamp() < this.fromTimestamp.getRawUnixTimestamp() ? -1 : 1;
    }
  }

  public static class IterateAllEntry {
    public final OSHDBTimestamp timestamp;
    public final OSMEntity osmEntity;
    public final OSMEntity previousOsmEntity;
    public final LazyEvaluatedObject<Geometry> geometry;
    public final LazyEvaluatedObject<Geometry> previousGeometry;
    public final LazyEvaluatedContributionTypes activities;

    IterateAllEntry(
        OSHDBTimestamp timestamp,
        OSMEntity entity, OSMEntity previousOsmEntity,
        LazyEvaluatedObject<Geometry> geom, LazyEvaluatedObject<Geometry> previousGeometry,
        LazyEvaluatedContributionTypes activities
    ) {
      this.timestamp = timestamp;
      this.osmEntity = entity;
      this.previousOsmEntity = previousOsmEntity;
      this.geometry = geom;
      this.previousGeometry = previousGeometry;
      this.activities = activities;
    }
  }

  /**
   * Helper method to easily iterate over all entity modifications in a cell that match a given
   * condition/filter.
   *
   * @param cell the data cell
   * @param timeInterval time range of interest – only modifications inside this interval are
   *        included in the result
   *
   * @return a stream of matching filtered OSMEntities with their clipped Geometries and timestamp
   *         intervals.
   */
  public Stream<IterateAllEntry> iterateByContribution(
      GridOSHEntity cell, TimestampInterval timeInterval
  ) {
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
              !oshEntity.intersectsBbox(boundingBox) ||
              (isBoundByPolygon && bboxOutsidePolygon.test(oshEntity.getBoundingBox()))
      )) {
        // this osh entity doesn't match the prefilter or is fully outside the requested
        // area of interest -> skip it
        continue;
      }

      boolean fullyInside = allFullyInside || (
          oshEntity.insideBbox(boundingBox) &&
          (!isBoundByPolygon || bboxInPolygon.test(boundingBox))
      );

      List<OSHDBTimestamp> modTs = oshEntity.getModificationTimestamps(osmEntityFilter, true);

      if (modTs.size() == 0 || !timeInterval
          .intersects(new TimestampInterval(modTs.get(0), modTs.get(modTs.size() - 1)))) {
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
                osmEntity, prev.osmEntity,
                new LazyEvaluatedObject<>((Geometry)null), prev.geometry,
                new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.DELETION))
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
                  osmEntity, prev.osmEntity,
                  new LazyEvaluatedObject<>((Geometry)null), prev.geometry,
                  new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.DELETION))
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
              prev = new IterateAllEntry(timestamp, osmEntity, prev.osmEntity,
                  new LazyEvaluatedObject<>((Geometry)null), prev.geometry,
                  new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.DELETION))
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
                  if (prevEntity.getTags().length != osmEntity.getTags().length) {
                    tagsChange = true;
                  } else {
                    for (int i = 0; i < prevEntity.getTags().length; i++) {
                      if (prevEntity.getTags()[i] != osmEntity.getTags()[i]) {
                        tagsChange = true;
                        break;
                      }
                    }
                  }
                  return tagsChange;
                case MEMBERLIST_CHANGE:
                  // look if members have been changed between versions
                  boolean membersChange = false;
                  switch (prevEntity.getType()) {
                    case WAY:
                      OSMMember[] prevNds = ((OSMWay) prevEntity).getRefs();
                      OSMMember[] currNds = ((OSMWay) osmEntity).getRefs();
                      if (prevNds.length != currNds.length) {
                        membersChange = true;
                      } else {
                        for (int i = 0; i < prevNds.length; i++) {
                          if (prevNds[i].getId() != currNds[i].getId()) {
                            membersChange = true;
                            break;
                          }
                        }
                      }
                      break;
                    case RELATION:
                      OSMMember[] prevMembers = ((OSMRelation) prevEntity).getMembers();
                      OSMMember[] currMembers = ((OSMRelation) osmEntity).getMembers();
                      if (prevMembers.length != currMembers.length) {
                        membersChange = true;
                      } else {
                        for (int i = 0; i < prevMembers.length; i++) {
                          if (prevMembers[i].getId() != currMembers[i].getId()
                              || prevMembers[i].getType() != currMembers[i].getType()
                              || prevMembers[i].getRoleId() != currMembers[i].getRoleId()) {
                            membersChange = true;
                            break;
                          }
                        }
                      }
                      break;
                  }
                  return membersChange;
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
          if (prev != null) {
            result = new IterateAllEntry(timestamp,
                osmEntity, prev.osmEntity,
                geom, prev.geometry,
                activity
            );
          } else {
            result = new IterateAllEntry(timestamp,
                osmEntity, null,
                geom, new LazyEvaluatedObject<Geometry>((Geometry)null),
                activity
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

  /**
   * @deprecated renamed to {@link #iterateByContribution(GridOSHEntity, TimestampInterval)}
   */
  @Deprecated
  public Stream<IterateAllEntry> iterateAll(GridOSHEntity cell, TimestampInterval timeInterval) {
    return this.iterateByContribution(cell, timeInterval);
  }
}
