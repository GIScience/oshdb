package org.heigit.bigspatialdata.oshdb.util;

import com.vividsolutions.jts.geom.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;


public class CellIterator {

  /**
   * Helper method to easily iterate over all entities in a cell that match a given condition/filter.
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
  public static Stream<Map<Long, Pair<OSMEntity, Geometry>>> iterateAll(GridOSHEntity cell, BoundingBox boundingBox, List<Long> timestamps, TagInterpreter tagInterpreter, Predicate<OSMEntity> osmEntityFilter, boolean includeOldStyleMultipolygons) {
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

}
