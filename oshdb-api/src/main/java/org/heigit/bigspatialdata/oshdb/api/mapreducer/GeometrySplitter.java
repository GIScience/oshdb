package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;
import com.vividsolutions.jts.geom.TopologyException;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastBboxInPolygon;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastBboxOutsidePolygon;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastPolygonOperations;

/**
 * Helper class to split "MapReducible" objects into sub-regions of an area of interest.
 *
 * @param <U> an arbitrary index type to identify supplied sub-regions
 */
class GeometrySplitter<U extends Comparable<U>> implements Serializable {
  private static final long serialVersionUID = 1L;

  private STRtree spatialIndex = new STRtree();
  private Map<U, FastBboxInPolygon> bips = new HashMap<>();
  private Map<U, FastBboxOutsidePolygon> bops = new HashMap<>();
  private Map<U, FastPolygonOperations> poops = new HashMap<>();

  private Map<U, ? extends Geometry> subregions;

  <P extends Geometry & Polygonal> GeometrySplitter(Map<U, P> subregions) {
    subregions.forEach((index, geometry) -> {
      spatialIndex.insert(geometry.getEnvelopeInternal(), index);
      bips.put(index, new FastBboxInPolygon(geometry));
      bops.put(index, new FastBboxOutsidePolygon(geometry));
      poops.put(index, new FastPolygonOperations(geometry));
    });
    this.subregions = subregions;
  }

  /**
   * Splits osm entity snapshot objects into sub-regions.
   *
   * @param data the OSMEntitySnapshot to split into the given sub-regions
   * @return a list of OSMEntitySnapshot objects
   */
  public List<Pair<U, OSMEntitySnapshot>> splitOSMEntitySnapshot(OSMEntitySnapshot data) {
    OSHDBBoundingBox oshBoundingBox = data.getOSHEntity().getBoundingBox();
    //noinspection unchecked – STRtree works with raw types unfortunately :-/
    List<U> candidates = (List<U>) spatialIndex.query(
        OSHDBGeometryBuilder.getGeometry(oshBoundingBox).getEnvelopeInternal()
    );
    return candidates.stream()
        .filter(index -> !bops.get(index).test(oshBoundingBox)) // fully outside -> skip
        .flatMap(index -> {
          if (bips.get(index).test(oshBoundingBox)) {
            return Stream.of(new ImmutablePair<>(index, data)); // fully inside -> directly return
          }
          FastPolygonOperations poop = poops.get(index);
          try {
            Geometry intersection = poop.intersection(data.getGeometry());
            if (intersection == null || intersection.isEmpty()) {
              return Stream.empty(); // not actually intersecting -> skip
            } else {
              return Stream.of(new ImmutablePair<>(
                  index,
                  new OSMEntitySnapshot(data, intersection)
              ));
            }
          } catch (TopologyException ignored) {
            return Stream.empty(); // JTS cannot handle broken osm geometry -> skip
          }
        }).collect(Collectors.toCollection(LinkedList::new));
  }

  /**
   * Splits osm contributions into sub-regions.
   *
   * <p>
   * The original contribution type is preserved during this operation.
   * For example, when a building was moved inside the area of interest from sub-region A into sub-
   * region B, there will be two contribution objects in the result, both of type "geometry change"
   * (note that in this case the contribution object of sub-region A will …
   * todo: ^ is this the right behaviour of contribution types when splitting into sub-regions?
   * </p>
   *
   * @param data the OSMContribution to split into the given sub-regions
   * @return a list of OSMContribution objects
   */
  public List<Pair<U, OSMContribution>> splitOSMContribution(OSMContribution data) {
    OSHDBBoundingBox oshBoundingBox = data.getOSHEntity().getBoundingBox();
    //noinspection unchecked – STRtree works with raw types unfortunately :-/
    List<U> candidates = (List<U>) spatialIndex.query(
        OSHDBGeometryBuilder.getGeometry(oshBoundingBox).getEnvelopeInternal()
    );
    return candidates.stream()
        .filter(index -> !bops.get(index).test(oshBoundingBox)) // fully outside -> skip
        .flatMap(index -> {
          if (bips.get(index).test(oshBoundingBox)) {
            return Stream.of(new ImmutablePair<>(index, data)); // fully inside -> directly return
          }
          FastPolygonOperations poop = poops.get(index);
          try {
            Geometry intersectionBefore = poop.intersection(data.getGeometryBefore());
            Geometry intersectionAfter = poop.intersection(data.getGeometryAfter());
            if ((intersectionBefore == null || intersectionBefore.isEmpty())
                && (intersectionAfter == null || intersectionAfter.isEmpty())) {
              return Stream.empty(); // not actually intersecting -> skip
            } else {
              return Stream.of(new ImmutablePair<>(
                  index,
                  new OSMContribution(data, intersectionBefore, intersectionAfter)
              ));
            }
          } catch (TopologyException ignored) {
            return Stream.empty(); // JTS cannot handle broken osm geometry -> skip
          }
        }).collect(Collectors.toCollection(LinkedList::new));
  }

  /**
   * Custom object serialization/deserialization.
   *
   * <p>
   * Sometimes, a GeometrySplitter can end up containing quite many deeply nested child-objects.
   * Which can lead to relatively slow object serialization. It is then faster to just transfer
   * the geometries and re-create the indices at the destination after de-serializing.
   * </p>
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    WKBWriter writer = new WKBWriter();
    out.writeInt(this.subregions.size());
    for (Entry<U, ? extends Geometry> entry : this.subregions.entrySet()) {
      out.writeObject(entry.getKey());
      byte[] data = writer.write(entry.getValue());
      out.writeInt(data.length);
      out.write(data);
    }
  }

  private <P extends Geometry & Polygonal> void readObject(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    WKBReader reader = new WKBReader();
    int numEntries = in.readInt();
    TreeMap<U, P> result = new TreeMap<>();
    for (int i = 0; i < numEntries; i++) {
      //noinspection unchecked - we only write `U` data in these places in `writeObject`
      U key = (U) in.readObject();
      int dataLength = in.readInt();
      byte[] data = new byte[dataLength];
      int bytesRead = in.read(data);
      assert bytesRead == dataLength : "fewer bytes read than expected";
      try {
        //noinspection unchecked - we only write `P` data in these places in `writeObject`
        result.put(key, (P) reader.read(data));
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }
    this.subregions = result;
  }

  private <P extends Geometry & Polygonal> Object readResolve() throws ObjectStreamException {
    //noinspection unchecked - constructor checks that `subregions` only contain `P` entry values
    return new GeometrySplitter<>((Map<U, P>) this.subregions);
  }
}