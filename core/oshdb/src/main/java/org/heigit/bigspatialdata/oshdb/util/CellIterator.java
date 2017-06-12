package org.heigit.bigspatialdata.oshdb.util;

import com.vividsolutions.jts.geom.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;


public class CellIterator {

  /**
   * Helper method to easily iterate over all entities in a cell that match a given condition/filter as they existed at the given timestamps.
   *
   * @param cell the data cell
   * @param boundingBox only entities inside or intersecting this bbox are returned, geometries are clipped to this extent
   * @param timestamps a list of timestamps to return data for
   * @param osmEntityFilter a lambda called for each entity. if it returns true, the particular feature is included in the output
   * @param includeOldStyleMultipolygons if true, output contains also data for "old style multipolygons".
   *
   * Note, that if includeOldStyleMultipolygons is true, for each old style multipolygon only the geometry of the inner
   * holes are returned (while the outer part is already present as the respective way's output)! This has to be
   * interpreted separately and differently in the data analysis!
   * The includeOldStyleMultipolygons is also quite a bit less efficient (both CPU and memory) as the default path.
   *
   * @return a stream of matching filtered OSMEntities with their clipped Geometries at each timestamp.
   * If an object has not been modified between timestamps, the output may contain the *same* Geometry object in the
   * output multiple times. This can be used to optimize away recalculating expensive geometry operations on unchanged
   * feature geometries later on in the code.
   */
  public static Stream<Map<Long, Pair<OSMEntity, Geometry>>> iterateByTimestamps(GridOSHEntity cell, BoundingBox boundingBox, List<Long> timestamps, TagInterpreter tagInterpreter, Predicate<OSMEntity> osmEntityFilter, boolean includeOldStyleMultipolygons) {
    List<Map<Long, Pair<OSMEntity, Geometry>>> results = new ArrayList<>();

    for (OSHEntity<OSMEntity> oshEntity : (Iterable<OSHEntity<OSMEntity>>) cell) {
      if (!oshEntity.intersectsBbox(boundingBox)) {
        // this osh entity is fully outside the requested bounding box -> skip it
        continue;
      }
      boolean fullyInside = oshEntity.insideBbox(boundingBox);

      // optimize loop by requesting modification timestamps first, and skip geometry calculations where not needed
      SortedMap<Long, List<Long>> queryTs = new TreeMap<>();
      if (!includeOldStyleMultipolygons) {
        List<Long> modTs = oshEntity.getModificationTimestamps(osmEntityFilter);
        int j = 0;
        for (long requestedT : timestamps) {
          boolean needToRequest = false;
          while (j < modTs.size() && modTs.get(j) < requestedT) {
            needToRequest = true;
            j++;
          }
          if (needToRequest)
            queryTs.put(requestedT, new LinkedList<>());
          else if (queryTs.size() > 0)
            queryTs.get(queryTs.lastKey()).add(requestedT);
        }
      } else {
        // todo: make this work with old style multipolygons!!?!
        for (Long ts : timestamps)
          queryTs.put(ts, new LinkedList<>());
      }

      SortedMap<Long, OSMEntity> osmEntityByTimestamps = oshEntity.getByTimestamps(new ArrayList<>(queryTs.keySet()));
      Map<Long, Pair<OSMEntity, Geometry>> oshResult = new TreeMap<>();

      osmEntityLoop:
      for (Map.Entry<Long, OSMEntity> entity : osmEntityByTimestamps.entrySet()) {
        Long timestamp = entity.getKey();
        OSMEntity osmEntity = entity.getValue();

        if (!osmEntity.isVisible()) {
          // skip because this entity is deleted at this timestamp
          continue;
        }

        boolean isOldStyleMultipolygon = false;
        if (includeOldStyleMultipolygons &&
            osmEntity instanceof OSMRelation &&
            tagInterpreter.isOldStyleMultipolygon((OSMRelation) osmEntity)
            ) {
          OSMRelation rel = (OSMRelation) osmEntity;
          for (int i = 0; i < rel.getMembers().length; i++) {
            if (rel.getMembers()[i].getType() == OSHEntity.WAY && tagInterpreter.isMultipolygonOuterMember(rel.getMembers()[i])) {
              OSMEntity way = rel.getMembers()[i].getEntity().getByTimestamp(timestamp);
              if (!osmEntityFilter.test(way)) {
                // skip this old-style-multipolygon because it doesn't match our filter
                continue osmEntityLoop;
              } else {
                // we know this multipolygon only has exactly one outer way, so we can abort the loop and actually
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
          Geometry geom;
          if (!isOldStyleMultipolygon) {
            geom = fullyInside ?
                osmEntity.getGeometry(timestamp, tagInterpreter) :
                osmEntity.getGeometryClipped(timestamp, tagInterpreter, boundingBox);
          } else {
            // old style multipolygons: return only the inner holes of the geometry -> this is then used to "fix" the
            // results obtained from calculating the geometry on the object's outer way which doesn't know about the
            // inner members of the multipolygon relation
            // todo: check if this is all valid?
            GeometryFactory gf = new GeometryFactory();
            geom = osmEntity.getGeometry(timestamp, tagInterpreter);
            Polygon poly = (Polygon) geom;
            Polygon[] interiorRings = new Polygon[poly.getNumInteriorRing()];
            for (int i = 0; i < poly.getNumInteriorRing(); i++)
              interiorRings[i] = new Polygon((LinearRing) poly.getInteriorRingN(i), new LinearRing[]{}, gf);
            geom = new MultiPolygon(interiorRings, gf);
            if (!fullyInside)
              geom = Geo.clip(geom, boundingBox);
          }

          if (geom == null) throw new NotImplementedException(); // todo: fix this hack!
          if (geom.isEmpty()) throw new NotImplementedException(); // todo: fix this hack!
          //if (!(geom.getGeometryType() == "Polygon" || geom.getGeometryType() == "MultiPolygon")) throw new NotImplementedException(); // hack! // todo: wat?

          oshResult.put(timestamp, new ImmutablePair<>(osmEntity, geom));
        } catch (UnsupportedOperationException err) {
          // e.g. unsupported relation types go here
        } catch (NotImplementedException err) {
          // todo: what to do here???
        } catch (IllegalArgumentException err) {
          System.err.printf("Relation %d skipped because of invalid geometry at timestamp %d\n", osmEntity.getId(), timestamp);
        } catch (TopologyException err) {
          System.err.printf("Topology error at object %d at timestamp %d: %s\n", osmEntity.getId(), timestamp, err.toString());
        }
      }

      // add skipped timestamps (where nothing has changed from the last timestamp) to set of results
      for (Map.Entry<Long, List<Long>> entry : queryTs.entrySet()) {
        Long key = entry.getKey();
        if (oshResult.containsKey(key)) { // could be missing in case this version
          Pair<OSMEntity, Geometry> existingResult = oshResult.get(key);
          for (Long additionalTs : entry.getValue()) {
            oshResult.put(additionalTs, existingResult);
          }
        }
      }

      results.add(oshResult);
    }

    // return as an obj stream
    return results.stream();
  }

  public static class IterateAllEntry {
    public final Long validFrom;
    public final Long validTo;
    public final OSMEntity osmEntity;
    public final OSMEntity previousOsmEntity;
    public final Geometry geometry;
    public final Geometry previousGeometry;
    public final EnumSet<ActivityType> activities;
    IterateAllEntry(Long from, Long to, OSMEntity entity, OSMEntity previousOsmEntity, Geometry geom, Geometry previousGeometry, EnumSet<ActivityType> activities) {
      this.validFrom = from;
      this.validTo = to;
      this.osmEntity = entity;
      this.previousOsmEntity = previousOsmEntity;
      this.geometry = geom;
      this.previousGeometry = previousGeometry;
      this.activities = activities;
    }
    public enum ActivityType {
      CREATION, // a new object has been created
      DELETION, // one object has been deleted
      TAG_CHANGE, // at least one tag of this object has been modified
      MEMBERLIST_CHANGE, // the member list of this object (way or relation) has changed in some way
      GEOMETRY_CHANGE // the geometry of the object has been modified either directly (see MEMBERLIST_CHANGE) or via changed coordinates of the object's child entities
    }
  }
  /**
   * Helper method to easily iterate over all entities in a cell that match a given condition/filter.
   *
   * @param cell the data cell
   * @param boundingBox only entities inside or intersecting this bbox are returned, geometries are clipped to this extent
   * @param osmEntityFilter a lambda called for each entity. if it returns true, the particular feature is included in the output
   * @param includeOldStyleMultipolygons if true, output contains also data for "old style multipolygons".
   *
   * Note, that if includeOldStyleMultipolygons is true, for each old style multipolygon only the geometry of the inner
   * holes are returned (while the outer part is already present as the respective way's output)! This has to be
   * interpreted separately and differently in the data analysis!
   * The includeOldStyleMultipolygons is also quite a bit less efficient (both CPU and memory) as the default path.
   *
   * @return a stream of matching filtered OSMEntities with their clipped Geometries and timestamp intervals.
   */
  public static Stream<IterateAllEntry> iterateAll(GridOSHEntity cell, BoundingBox boundingBox, TagInterpreter tagInterpreter, Predicate<OSMEntity> osmEntityFilter, boolean includeOldStyleMultipolygons) {
    List<IterateAllEntry> results = new LinkedList<>();
    if (includeOldStyleMultipolygons)
      throw new Error("this is not yet properly implemented (probably)"); //todo: remove this by finishing the functionality below

    for (OSHEntity<OSMEntity> oshEntity : (Iterable<OSHEntity<OSMEntity>>) cell) {
      if (!oshEntity.intersectsBbox(boundingBox)) {
        // this osh entity is fully outside the requested bounding box -> skip it
        continue;
      }
      boolean fullyInside = oshEntity.insideBbox(boundingBox);

      List<Long> modTs = oshEntity.getModificationTimestamps(osmEntityFilter);
      SortedMap<Long, OSMEntity> osmEntityByTimestamps = oshEntity.getByTimestamps(modTs);

      osmEntityLoop:
      for (Map.Entry<Long, OSMEntity> entity : osmEntityByTimestamps.entrySet()) {
        Long timestamp = entity.getKey();
        OSMEntity osmEntity = entity.getValue();

        IterateAllEntry prev = results.size() > 0 ? results.get(results.size()-1) : null; // todo: replace with variable outside of osmEntitiyLoop (than we can also get rid of the ` || prev.osmEntity.getId() != osmEntity.getId()`'s below)
        Long nextTs = null;
        if (modTs.size() > modTs.indexOf(timestamp)+1) //todo: better way to figure out if timestamp is not last element??
          nextTs = modTs.get(modTs.indexOf(timestamp)+1);

        if (!osmEntity.isVisible()) {
          // this entity is deleted at this timestamp
          if (prev != null && prev.osmEntity.getId() == osmEntity.getId() && !prev.activities.contains(IterateAllEntry.ActivityType.DELETION)) // todo: some of this may be refactorable between the two for loops
            results.add(new IterateAllEntry(timestamp, nextTs, osmEntity, prev.osmEntity, null, prev.geometry, EnumSet.of(IterateAllEntry.ActivityType.DELETION)));
          continue;
        }

        // todo check old style mp code!!1!!!11!
        boolean isOldStyleMultipolygon = false;
        if (includeOldStyleMultipolygons &&
            osmEntity instanceof OSMRelation &&
            tagInterpreter.isOldStyleMultipolygon((OSMRelation) osmEntity)
            ) {
          OSMRelation rel = (OSMRelation) osmEntity;
          for (int i = 0; i < rel.getMembers().length; i++) {
            if (rel.getMembers()[i].getType() == OSHEntity.WAY && tagInterpreter.isMultipolygonOuterMember(rel.getMembers()[i])) {
              OSMEntity way = rel.getMembers()[i].getEntity().getByTimestamp(timestamp);
              if (!osmEntityFilter.test(way)) {
                // skip this old-style-multipolygon because it doesn't match our filter
                continue osmEntityLoop;
              } else {
                // we know this multipolygon only has exactly one outer way, so we can abort the loop and actually
                // "continue" with the calculations ^-^
                isOldStyleMultipolygon = true;
                break;
              }
            }
          }
        } else {
          if (!osmEntityFilter.test(osmEntity)) {
            // this entity doesn't match our filter (anymore)
            // TODO?: separate/additional activity type (e.g. "RECYCLED" ??) and still construct geometries for these?
            if (prev != null && prev.osmEntity.getId() == osmEntity.getId() && !prev.activities.contains(IterateAllEntry.ActivityType.DELETION))
              results.add(new IterateAllEntry(timestamp, nextTs, osmEntity, prev.osmEntity, null, prev.geometry, EnumSet.of(IterateAllEntry.ActivityType.DELETION)));
            continue osmEntityLoop;
          }
        }

        try {
          Geometry geom;
          if (!isOldStyleMultipolygon) {
            geom = fullyInside ?
                osmEntity.getGeometry(timestamp, tagInterpreter) :
                osmEntity.getGeometryClipped(timestamp, tagInterpreter, boundingBox);
          } else {
            // old style multipolygons: return only the inner holes of the geometry -> this is then used to "fix" the
            // results obtained from calculating the geometry on the object's outer way which doesn't know about the
            // inner members of the multipolygon relation
            // todo: check if this is all valid?
            GeometryFactory gf = new GeometryFactory();
            geom = osmEntity.getGeometry(timestamp, tagInterpreter);
            Polygon poly = (Polygon) geom;
            Polygon[] interiorRings = new Polygon[poly.getNumInteriorRing()];
            for (int i = 0; i < poly.getNumInteriorRing(); i++)
              interiorRings[i] = new Polygon((LinearRing) poly.getInteriorRingN(i), new LinearRing[]{}, gf);
            geom = new MultiPolygon(interiorRings, gf);
            if (!fullyInside)
              geom = Geo.clip(geom, boundingBox);
          }

          if (geom == null) throw new NotImplementedException(); // todo: fix this hack!
          if (geom.isEmpty()) throw new NotImplementedException(); // todo: fix this hack!
          //if (!(geom.getGeometryType() == "Polygon" || geom.getGeometryType() == "MultiPolygon")) throw new NotImplementedException(); // hack! // todo: wat?

          EnumSet<IterateAllEntry.ActivityType> activity;
          if (prev == null || prev.osmEntity.getId() != osmEntity.getId()) {
            activity = EnumSet.of(IterateAllEntry.ActivityType.CREATION);
          } else {
            activity = EnumSet.noneOf(IterateAllEntry.ActivityType.class);
            // look if tags have been changed between versions
            boolean tagsChange = false;
            if (prev.osmEntity.getTags().length != osmEntity.getTags().length)
              tagsChange = true;
            else
              for (int i=0; i<prev.osmEntity.getTags().length; i++)
                if (prev.osmEntity.getTags()[i] != osmEntity.getTags()[i]) {
                  tagsChange = true;
                  break;
                }
            if (tagsChange) activity.add(IterateAllEntry.ActivityType.TAG_CHANGE);
            // look if members have been changed between versions
            boolean membersChange = false;
            switch (prev.osmEntity.getType()) {
              case OSHEntity.WAY:
                OSMMember[] prevNds = ((OSMWay)prev.osmEntity).getRefs();
                OSMMember[] currNds = ((OSMWay)osmEntity).getRefs();
                if (prevNds.length != currNds.length)
                  membersChange = true;
                else
                  for (int i=0; i<prevNds.length; i++)
                    if (prevNds[i].getId() != currNds[i].getId()) {
                      membersChange = true;
                      break;
                    }
                break;
              case OSHEntity.RELATION:
                OSMMember[] prevMembers = ((OSMRelation)prev.osmEntity).getMembers();
                OSMMember[] currMembers = ((OSMRelation)osmEntity).getMembers();
                if (prevMembers.length != currMembers.length)
                  membersChange = true;
                else
                  for (int i=0; i<prevMembers.length; i++)
                    if (prevMembers[i].getId() != currMembers[i].getId() ||
                        prevMembers[i].getType() != currMembers[i].getType() ||
                        prevMembers[i].getRoleId() != currMembers[i].getRoleId()) {
                      membersChange = true;
                      break;
                    }
                break;
            }
            if (membersChange) activity.add(IterateAllEntry.ActivityType.MEMBERLIST_CHANGE);
            // look if geometry has been changed between versions
            boolean geometryChange = false;
            if (geom != null && prev.osmEntity.getId() == osmEntity.getId() && prev.geometry != null) // todo: what if both are null? -> maybe fall back to MEMEBER_CHANGE?
              geometryChange = !prev.geometry.equals(geom); // todo: check: does this work as expected?
            if (geometryChange) activity.add(IterateAllEntry.ActivityType.GEOMETRY_CHANGE);
          }

          if (prev != null && prev.osmEntity.getId() == osmEntity.getId())
            results.add(new IterateAllEntry(timestamp, nextTs, osmEntity, prev.osmEntity, geom, prev.geometry, activity));
          else
            results.add(new IterateAllEntry(timestamp, nextTs, osmEntity, null, geom, null, activity));
        } catch (UnsupportedOperationException err) {
          // e.g. unsupported relation types go here
        } catch (NotImplementedException err) {
          // todo: what to do here???
        } catch (IllegalArgumentException err) {
          System.err.printf("Relation %d skipped because of invalid geometry at timestamp %d\n", osmEntity.getId(), timestamp);
        } catch (TopologyException err) {
          System.err.printf("Topology error at object %d at timestamp %d: %s\n", osmEntity.getId(), timestamp, err.toString());
        }
      }
    }

    // return as an obj stream
    return results.stream();
  }

}
