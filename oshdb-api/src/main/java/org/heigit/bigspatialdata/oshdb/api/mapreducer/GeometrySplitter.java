package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.celliterator.LazyEvaluatedObject;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastBboxInPolygon;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastBboxOutsidePolygon;
import org.heigit.bigspatialdata.oshdb.util.geometry.fip.FastPolygonOperations;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

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

  private static class IndexData<I, D> {
    private final I index;
    private final D data;

    IndexData(I index, D data) {
      this.index = index;
      this.data = data;
    }

    I getIndex() {
      return index;
    }

    D getData() {
      return data;
    }
  }

  /**
   * Splits osm entity snapshot objects into sub-regions.
   *
   * @param data the OSMEntitySnapshot to split into the given sub-regions
   * @return a list of OSMEntitySnapshot objects
   */
  public Map<U, OSMEntitySnapshot> splitOSMEntitySnapshot(OSMEntitySnapshot data) {
    OSHDBBoundingBox oshBoundingBox = data.getOSHEntity().getBoundingBox();
    @SuppressWarnings("unchecked") // STRtree works with raw types unfortunately
    List<U> candidates = (List<U>) spatialIndex.query(
        OSHDBGeometryBuilder.getGeometry(oshBoundingBox).getEnvelopeInternal()
    );
    return candidates.stream()
        // OSH entity fully outside -> skip
        .filter(index -> !bops.get(index).test(oshBoundingBox))
        .flatMap(index -> {
          if (bips.get(index).test(oshBoundingBox)) {
            // OSH entity fully inside -> directly return
            return Stream.of(new IndexData<>(index, data));
          }

          // now we can check against the actual contribution geometry
          Geometry snapshotGeometry = data.getGeometry();
          OSHDBBoundingBox snapshotBbox = OSHDBGeometryBuilder.boundingBoxOf(
              snapshotGeometry.getEnvelopeInternal()
          );

          // OSM entity fully outside -> skip
          if (bops.get(index).test(snapshotBbox)) {
            return Stream.empty();
          }
          // OSM entity fully inside -> directly return
          if (bips.get(index).test(snapshotBbox)) {
            return Stream.of(new IndexData<>(index, data));
          }

          FastPolygonOperations poop = poops.get(index);
          try {
            boolean intersects = poop.intersects(snapshotGeometry);
            if (!intersects) {
              return Stream.empty(); // not actually intersecting -> skip
            } else {
              return Stream.of(new IndexData<>(index, new OSMEntitySnapshot(data,
                  new LazyEvaluatedObject<>(() -> poop.intersection(snapshotGeometry))
              )));
            }
          } catch (TopologyException ignored) {
            return Stream.empty(); // JTS cannot handle broken osm geometry -> skip
          }
        }).collect(Collectors.toMap(IndexData::getIndex, IndexData::getData));
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
  public Map<U, OSMContribution> splitOSMContribution(OSMContribution data) {
    OSHDBBoundingBox oshBoundingBox = data.getOSHEntity().getBoundingBox();
    @SuppressWarnings("unchecked") // STRtree works with raw types unfortunately
    List<U> candidates = (List<U>) spatialIndex.query(
        OSHDBGeometryBuilder.getGeometry(oshBoundingBox).getEnvelopeInternal()
    );
    return candidates.stream()
        // OSH entity fully outside -> skip
        .filter(index -> !bops.get(index).test(oshBoundingBox))
        .flatMap(index -> {
          // OSH entity fully inside -> directly return
          if (bips.get(index).test(oshBoundingBox)) {
            return Stream.of(new IndexData<>(index, data));
          }

          // now we can check against the actual contribution geometry
          Geometry contributionGeometryBefore = data.getGeometryBefore();
          Geometry contributionGeometryAfter = data.getGeometryAfter();
          OSHDBBoundingBox contributionGeometryBbox;
          if (data.is(ContributionType.CREATION)) {
            contributionGeometryBbox = OSHDBGeometryBuilder.boundingBoxOf(
                contributionGeometryAfter.getEnvelopeInternal()
            );
          } else if (data.is(ContributionType.DELETION)) {
            contributionGeometryBbox = OSHDBGeometryBuilder.boundingBoxOf(
                contributionGeometryBefore.getEnvelopeInternal()
            );
          } else {
            contributionGeometryBbox = OSHDBGeometryBuilder.boundingBoxOf(
                contributionGeometryBefore.getEnvelopeInternal()
            );
            contributionGeometryBbox.add(OSHDBGeometryBuilder.boundingBoxOf(
                contributionGeometryAfter.getEnvelopeInternal()
            ));
          }

          // contribution fully outside -> skip
          if (bops.get(index).test(contributionGeometryBbox)) {
            return Stream.empty();
          }
          // contribution fully inside -> directly return
          if (bips.get(index).test(contributionGeometryBbox)) {
            return Stream.of(new IndexData<>(index, data));
          }

          FastPolygonOperations poop = poops.get(index);
          try {
            boolean intersectsBefore = poop.intersects(contributionGeometryBefore);
            boolean intersectsAfter = poop.intersects(contributionGeometryAfter);
            if ((!intersectsBefore) && (!intersectsAfter)) {
              return Stream.empty(); // not actually intersecting -> skip
            } else {
              return Stream.of(new IndexData<>(index, new OSMContribution(data,
                  new LazyEvaluatedObject<>(() -> poop.intersection(contributionGeometryBefore)),
                  new LazyEvaluatedObject<>(() -> poop.intersection(contributionGeometryAfter)))
              ));
            }
          } catch (TopologyException ignored) {
            return Stream.empty(); // JTS cannot handle broken osm geometry -> skip
          }
        }).collect(Collectors.toMap(IndexData::getIndex, IndexData::getData));
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
      @SuppressWarnings("unchecked") // we only write `U` data in these places in `writeObject`
      U key = (U) in.readObject();
      int dataLength = in.readInt();
      byte[] data = new byte[dataLength];
      int bytesRead = in.read(data);
      assert bytesRead == dataLength : "fewer bytes read than expected";
      try {
        @SuppressWarnings("unchecked") // we only write `P` data in these places in `writeObject`
        P readData = (P) reader.read(data);
        result.put(key, readData);
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }
    this.subregions = result;
  }

  protected <P extends Geometry & Polygonal> Object readResolve() throws ObjectStreamException {
    @SuppressWarnings("unchecked") // constructor checks that `subregions` only contain `P` entries
    Map<U, P> subregions = (Map<U, P>) this.subregions;
    return new GeometrySplitter<>(subregions);
  }
}