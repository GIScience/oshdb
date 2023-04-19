package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshotImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateByTimestampEntry;
import org.heigit.ohsome.oshdb.util.celliterator.LazyEvaluatedObject;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestampList;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

class TestAutoAggregation {
  private static final GeometryFactory geomFactory = new GeometryFactory();
  private static final Point point = geomFactory.createPoint(new Coordinate(10.0, 10.0));
  private static final Polygon area = (Polygon) geomFactory.toGeometry(new Envelope(0, 20, 0, 20));

  private static final OSHNode node = OSHNodeImpl.build(
      new ArrayList<>(List.of(OSM.node(1, 1, 1000, 1, 23, new int[0], 10_0000000, 10_0000000))));
  private static final List<OSHNode> nodes = List.of(node);
  private static final OSHDBTimestampList timestamps = new OSHDBTimestamps("2020-01-01");

  private static final OSHDBDatabase oshdb = new OSHDBDatabaseMock();

  @Test
  void testAggregateByGeometryThenMapSum() throws Exception {
    var geometries = Map.of("TEST", area);
    var mr = new MapReducerMock<>(oshdb, OSMEntitySnapshot.class);

    SortedMap<String, Number> result = mr.timestamps("2020-01-01").aggregateByGeometry(geometries)
        .map(s -> s.getEntity().getUserId())
        .sum();

    assertEquals(1, result.size());
    assertEquals(23, result.get("TEST").intValue());
  }

  @Test
  void testMapThenAggregateByGeometrySum() throws Exception {
    var geometries = Map.of("TEST", area);
    var mr = new MapReducerMock<>(oshdb, OSMEntitySnapshot.class);

    SortedMap<String, Number> result = mr.timestamps("2020-01-01")
        .map(s -> s.getEntity().getUserId())
        .aggregateByGeometry(geometries)
        .sum();

    assertEquals(1, result.size());
    assertEquals(23, result.get("TEST").intValue());
  }

  @Test
  void testMapThenAggregateByGeometryCollect() throws Exception {
    var geometries = Map.of("TEST", area);
    var mr = new MapReducerMock<>(oshdb, OSMEntitySnapshot.class);

    SortedMap<String, List<Integer>> result = mr.timestamps("2020-01-01")
        .map(s -> s.getEntity().getUserId())
        .aggregateByGeometry(geometries)
        .collect();

    assertEquals(1, result.size());
    assertEquals(23, result.get("TEST").get(0));
  }

  /**
   * Mocks and helper functions.
   *
   */
  private static class OSHDBDatabaseMock extends OSHDBDatabase {

    public OSHDBDatabaseMock(){
      super("");
    }

    @Override
    public TagTranslator getTagTranslator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

    @Override
    public <X extends OSHDBMapReducible> MapReducer<X> createMapReducer(Class<X> forClass) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String metadata(String property) {
      throw new UnsupportedOperationException();
    }
  }

  private static class MapReducerMock<X extends OSHDBMapReducible> extends MapReducer<X> {

    protected MapReducerMock(OSHDBDatabase oshdb, Class<X> viewClass) {
      super(oshdb, viewClass);
    }

    private MapReducerMock(MapReducerMock<X> other) {
      super(other);
    }

    @NotNull
    @Override
    protected MapReducer<X> copy() {
      return new MapReducerMock<>(this);
    }

    @Override
    protected Stream<X> mapStreamCellsOSMContribution(
        SerializableFunction<OSMContribution, X> mapper)  {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Stream<X> flatMapStreamCellsOSMContributionGroupedById(
        SerializableFunction<List<OSMContribution>, Iterable<X>> mapper) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Stream<X> mapStreamCellsOSMEntitySnapshot(
        SerializableFunction<OSMEntitySnapshot, X> mapper) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
        SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected <R, S> S mapReduceCellsOSMContribution(
        SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
        SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected <R, S> S mapReduceCellsOSMEntitySnapshot(
        SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      return nodes.stream().map(TestAutoAggregation::snapshot).map(mapper).reduce(identitySupplier.get(),
          accumulator, combiner);
    }

    @Override
    protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
        SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      return Streams.stream(mapper.apply(nodes.stream()
          .map(TestAutoAggregation::snapshot)
          .collect(Collectors.toList())))
          .reduce(identitySupplier.get(), accumulator, combiner);
    }
  }

  private static OSMEntitySnapshot snapshot(OSHNode node) {
    var timestamp = timestamps.get().first();
    var data = new IterateByTimestampEntry(timestamp, null,
        node.getVersions().iterator().next(), node,
        new LazyEvaluatedObject<>(point),
        new LazyEvaluatedObject<>(point));
    return new OSMEntitySnapshotImpl(data);
  }

}
