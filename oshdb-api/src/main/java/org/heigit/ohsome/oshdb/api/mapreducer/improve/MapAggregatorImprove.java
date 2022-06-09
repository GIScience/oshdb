package org.heigit.ohsome.oshdb.api.mapreducer.improve;

import static java.util.Map.entry;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

class MapAggregatorImprove<U extends Comparable<U> & Serializable, X>
    implements MapAggregator<U, X> {
  private MapReducer<Map.Entry<U, X>> mapReducer;

  private MapAggregatorImprove(MapReducer<Map.Entry<U, X>> mapReducer) {
    this.mapReducer = mapReducer;
  }

  MapAggregatorImprove(MapReducer<X> mapReducer, SerializableFunction<X, U> indexer) {
    this(mapReducer.map(x -> entry(indexer.apply(x), x)));
  }

  @Override
  public <V extends Comparable<V> & Serializable>
      MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateBy(
          SerializableFunction<X, V> indexer, Collection<V> zerofill) {
    return new MapAggregatorImprove<>(
        mapReducer.map(e -> entry(new OSHDBCombinedIndex<>(e.getKey(), indexer.apply(e.getValue())),
            e.getValue())));
  }

  @Override
  public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal> MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateByGeometry(
      Map<V, P> geometries) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <R> MapAggregator<U, R> map(SerializableFunction<X, R> map) {
    var mr = mapReducer.map(ux -> entry(ux.getKey(), map.apply(ux.getValue())));
    return new MapAggregatorImprove<>(mr);
  }

  @Override
  public <R> MapAggregator<U, R> flatMap(SerializableFunction<X, Stream<R>> map) {
    var mr = mapReducer.flatMap(ux -> map.apply(ux.getValue()).map(r -> entry(ux.getKey(), r)));
    return new MapAggregatorImprove<>(mr);
  }

  @Override
  public MapAggregator<U, X> filter(SerializablePredicate<X> f) {
    var mr = mapReducer.filter(data -> f.test(data.getValue()));
    return new MapAggregatorImprove<>(mr);
  }

  @Override
  public <S> SortedMap<U, S> reduce(SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) {
    return mapReducer
        .reduce(TreeMap::new,
            (map, entry) -> {
              map.compute(entry.getKey(), (k, v) -> v == null
                  ? accumulator.apply(identitySupplier.get(), entry.getValue()) :
                    accumulator.apply(v, entry.getValue()));
              return map; },
            (a, b) -> {
              var map = new TreeMap<U, S>();
              map.putAll(a);
              b.forEach((k, v) -> map.merge(k, v, combiner));
              return map; });
  }

  @Override
  public Stream<Entry<U, X>> stream() {
    return mapReducer.stream();
  }
}
