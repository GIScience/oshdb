package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastBboxInPolygon;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastBboxOutsidePolygon;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastPolygonOperations;

/**
 * helper class to split "MapReducible" objects into sub-regions of an area of interest.
 *
 * @param <U> an arbitrary index type to identify supplied sub-regions
 */
class GeometrySplitter<U extends Comparable<U>> {
  private Set<U> indices;

  private Map<U, FastBboxInPolygon> bips = new HashMap<>();
  private Map<U, FastBboxOutsidePolygon> bops = new HashMap<>();
  private Map<U, FastPolygonOperations> poops = new HashMap<>();

  <P extends Geometry & Polygonal> GeometrySplitter(Map<U, P> subregions) {
    subregions.forEach((index, geometry) -> {
      bips.put(index, new FastBboxInPolygon(geometry));
      bops.put(index, new FastBboxOutsidePolygon(geometry));
      poops.put(index, new FastPolygonOperations(geometry));
    });
    this.indices = subregions.keySet();
  }

  /**
   * splits osm entity snapshot objects into sub-regions
   *
   * @param data the OSMEntitySnapshot to split into the given sub-regions
   * @return a list of OSMEntitySnapshot objects
   */
  public List<Pair<U, OSMEntitySnapshot>> splitOSMEntitySnapshot(OSMEntitySnapshot data) {
    OSHDBBoundingBox oshBoundingBox = data.getOSHEntity().getBoundingBox();
    return indices.stream()
        // todo: optimization: rtree of subregion geometries -> limit indices to check -- worth it?
        .filter(index -> !bops.get(index).test(oshBoundingBox)) // fully outside -> skip
        .flatMap(index -> {
          if (bips.get(index).test(oshBoundingBox)) {
            return Stream.of(new ImmutablePair<>(index, data)); // fully inside -> directly return
          }
          FastPolygonOperations poop = poops.get(index);
          Geometry intersection = poop.intersection(data.getGeometry());
          if (intersection.isEmpty()) {
            return Stream.empty(); // not actually intersecting -> skip
          } else {
            return Stream.of(new ImmutablePair<>(
                index,
                new OSMEntitySnapshot(data, intersection)
            ));
          }
        }).collect(Collectors.toCollection(LinkedList::new));
  }

  /**
   * splits osm contributions into sub-regions
   *
   * The original contribution type is preserved during this operation.
   * For example, when a building was moved inside the area of interest from sub-region A into sub-
   * region B, there will be two contribution objects in the result, both of type "geometry change"
   * (note that in this case the contribution object of sub-region A will
   * todo: ^ is this the right behaviour of contribution types when splitting into sub-regions?
   *
   * @param data the OSMContribution to split into the given sub-regions
   * @return a list of OSMContribution objects
   */
  public List<Pair<U, OSMContribution>> splitOSMContribution(OSMContribution data) {
    OSHDBBoundingBox oshBoundingBox = data.getOSHEntity().getBoundingBox();
    return indices.stream()
        // todo: optimization: rtree of subregion geometries -> limit indices to check -- worth it?
        .filter(index -> !bops.get(index).test(oshBoundingBox)) // fully outside -> skip
        .flatMap(index -> {
          if (bips.get(index).test(oshBoundingBox))
            return Stream.of(new ImmutablePair<>(index, data)); // fully inside -> directly return
          FastPolygonOperations poop = poops.get(index);
          Geometry intersectionBefore = poop.intersection(data.getGeometryBefore());
          Geometry intersectionAfter = poop.intersection(data.getGeometryAfter());
          if ((intersectionBefore == null || intersectionBefore.isEmpty()) &&
              (intersectionAfter == null || intersectionAfter.isEmpty()))
            return Stream.empty(); // not actually intersecting -> skip
          else
            return Stream.of(new ImmutablePair<>(
                index,
                new OSMContribution(data, intersectionBefore, intersectionAfter)
            ));
        }).collect(Collectors.toCollection(LinkedList::new));
  }


}
