package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Polygon;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.generic.*;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.*;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A MapReducer with built-in aggregation by an arbitrary index
 *
 * This class provides similar functionality as a MapReducer, with the difference that here the `reduce` does
 * automatic aggregation of results by the values returned by an arbitrary indexing function.
 *
 * All results for which the set `indexer` returns the same value are aggregated into separate "bins".
 * This can be used to aggregate results by timestamp, geographic region, user id, osm tag, etc.
 *
 * Internally, this wraps around an existing MapReducer object, which still continues to be responsible for all actual calculations.
 *
 * @param <X> the type that is returned by the currently set of mapper function. the next added mapper function will be called with a parameter of this type as input
 * @param <U> the type of the index values returned by the `mapper function`, used to group results
 */
public class MapAggregator<U extends Comparable, X> {

  MapReducer<Pair<U, X>> _mapReducer;

  /**
   * basic constructor
   *
   * @param mapReducer mapReducer object which will be doing all actual calculations
   * @param indexer function that returns the index value into which to aggregate the respective result
   */
  MapAggregator(MapReducer<X> mapReducer, SerializableFunction<X, U> indexer) {
    this._mapReducer = mapReducer.map(data -> new MutablePair<U, X>(
        indexer.apply(data),
        data
    ));
  }

  /**
   * empty dummy constructor, used by MapBiAggregatorByTimestamps (which sets the _mapReducer property by itself)
   */
  MapAggregator() {}

  // -------------------------------------------------------------------------------------------------------------------
  // Filtering methods
  // Just forwards everything to the wrapped MapReducer object
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Set the area of interest to the given bounding box.
   * Only objects inside or clipped by this bbox will be passed on to the analysis' `mapper` function.
   *
   * @param bboxFilter the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapAggregator<U, X> areaOfInterest(BoundingBox bboxFilter) {
    this._mapReducer.areaOfInterest(bboxFilter);
    return this;
  }

  /**
   * Set the area of interest to the given polygon.
   * Only objects inside or clipped by this polygon will be passed on to the analysis' `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapAggregator<U, X> areaOfInterest(Polygon polygonFilter) {
    this._mapReducer.areaOfInterest(polygonFilter);
    return this;
  }

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param typeFilter the set of osm types to filter (e.g. `EnumSet.of(OSMType.WAY)`)
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapAggregator<U, X> osmTypes(EnumSet<OSMType> typeFilter) {
    this._mapReducer.osmTypes(typeFilter);
    return this;
  }

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param type1 the set of osm types to filter (e.g. `OSMType.NODE`)
   * @param otherTypes more osm types which should be analyzed
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapAggregator<U, X> osmTypes(OSMType type1, OSMType ...otherTypes) {
    return this.osmTypes(EnumSet.of(type1, otherTypes));
  }

  /**
   * Adds a custom arbitrary filter that gets executed for each osm entity and determines if it should be considered for this analyis or not.
   *
   * @param f the filter function to call for each osm entity
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapAggregator<U, X> where(SerializablePredicate<OSMEntity> f) {
    this._mapReducer.where(f);
    return this;
  }

  /**
   * Adds a custom arbitrary filter that gets executed for each osm entity and determines if it should be considered for this analyis or not.
   *
   * Deprecated, use `where(f)` instead.
   *
   * @param f the filter function to call for each osm entity
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @deprecated use `where(f)` instead
   */
  @Deprecated
  public MapAggregator<U, X> filterByOSMEntity(SerializablePredicate<OSMEntity> f) {
    return this.where(f);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key (with an arbitrary value).
   *
   * @param key the tag key to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @throws Exception
   */
  public MapAggregator<U, X> where(String key) throws Exception {
    this._mapReducer.where(key);
    return this;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key and value.
   *
   * @param key the tag key to filter the osm entities for
   * @param value the tag value to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @throws Exception
   */
  public MapAggregator<U, X> where(String key, String value) throws Exception {
    this._mapReducer.where(key, value);
    return this;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key (with an arbitrary value).
   *
   * Deprecated, use `where(key)` instead.
   *
   * @param key the tag key to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @throws Exception
   * @deprecated use `where(key)` instead
   */
  @Deprecated
  public MapAggregator<U, X> filterByTag(String key) throws Exception {
    return this.where(key);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key (with an arbitrary value).
   *
   * Deprecated, use `where(key,value)` instead.
   *
   * @param key the tag key to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @throws Exception
   * @deprecated use `where(key,value)` instead
   */
  @Deprecated
  public MapAggregator<U, X> filterByTag(String key, String value) throws Exception {
    return this.where(key, value);
  }

  // -------------------------------------------------------------------------------------------------------------------
  // "Quality of life" helper methods to use the map-reduce functionality more directly and easily for typical queries.
  // Available are: sum, count, average, weightedAverage and uniq.
  // Each one can be used to get results aggregated by timestamp, aggregated by a custom index and not aggregated totals.
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Sums up the results.
   *
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime exception will be thrown.
   *
   * @return the sum of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   * @throws Exception
   */
  public SortedMap<U, Number> sum() throws Exception {
    return this
        .makeNumeric()
        .reduce(
            () -> 0,
            (SerializableBiFunction<Number, Number, Number>) (x, y) -> NumberUtils.add(x, y),
            (SerializableBinaryOperator<Number>) (x, y) -> NumberUtils.add(x, y)
        );
  }

  /**
   * Sums up the results provided by a given `mapper` function.
   *
   * This is a shorthand for `.map(mapper).sum()`, with the difference that here the numerical return type of the `mapper` is ensured.
   *
   * @param mapper function that returns the numbers to sum up
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the summed up results of the `mapper` function
   * @throws Exception
   */
  public <R extends Number> SortedMap<U, R> sum(SerializableFunction<X, R> mapper) throws Exception {
    return this
        .map(mapper)
        .reduce(
            () -> (R) (Integer) 0,
            (SerializableBiFunction<R, R, R>)(x, y) -> NumberUtils.add(x,y),
            (SerializableBinaryOperator<R>)(x, y) -> NumberUtils.add(x,y)
        );
  }

  /**
   * Counts the number of results.
   *
   * @return the total count of features or modifications, summed up over all timestamps
   * @throws Exception
   */
  public SortedMap<U, Integer> count() throws Exception {
    return this.sum(ignored -> 1);
  }

  /**
   * Gets all unique values of the results.
   *
   * For example, this can be used together with the OSMContributionView to get the total amount of unique users editing specific feature types.
   *
   * @return the set of distinct values
   * @throws Exception
   */
  public SortedMap<U, Set<X>> uniq() throws Exception {
    return this
        .reduce(
            HashSet::new,
            (acc, cur) -> { acc.add(cur); return acc; },
            (a,b) -> { HashSet<X> result = new HashSet<>(a); result.addAll(b); return result; }
        );
  }

  /**
   * Gets all unique values of the results provided by a given mapper function.
   *
   * This is a shorthand for `.map(mapper).uniq()`.
   *
   * @param mapper function that returns some values
   * @param <R> the type that is returned by the `mapper` function
   * @return a set of distinct values returned by the `mapper` function
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   * @throws Exception
   */
  public <R> SortedMap<U, Set<R>> uniq(SerializableFunction<X, R> mapper) throws Exception {
    return this.map(mapper).uniq();
  }

  /**
   * Calculates the averages of the results.
   *
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime exception will be thrown.
   *
   * @return the average of the current data
   * @throws Exception
   */
  public SortedMap<U, Double> average() throws Exception {
    return this
        .makeNumeric()
        .average(n -> n);
  }

  /**
   * Calculates the average of the results provided by a given `mapper` function.
   *
   * @param mapper function that returns the numbers to average
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the average of the numbers returned by the `mapper` function
   * @throws Exception
   */
  public <R extends Number> SortedMap<U, Double> average(SerializableFunction<X, R> mapper) throws Exception {
    return this.weightedAverage(data -> new WeightedValue<>(mapper.apply(data), 1.0));
  }

  /**
   * Calculates the weighted average of the results provided by the `mapper` function.
   *
   * The mapper must return an object of the type `WeightedValue` which contains a numeric value associated with a (floating point) weight.
   *
   * @param mapper function that gets called for each entity snapshot or modification, needs to return the value and weight combination of numbers to average
   * @return the weighted average of the numbers returned by the `mapper` function
   * @throws Exception
   */
  public SortedMap<U, Double> weightedAverage(SerializableFunction<X, WeightedValue<Number>> mapper) throws Exception {
    return this
        .map(mapper)
        .reduce(
            () -> new PayloadWithWeight<>(0.0,0.0),
            (acc, cur) -> {
              acc.num = NumberUtils.add(acc.num, cur.getValue().doubleValue()*cur.getWeight());
              acc.weight += cur.getWeight();
              return acc;
            },
            (a, b) -> new PayloadWithWeight<>(NumberUtils.add(a.num, b.num), a.weight+b.weight)
        ).entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().num / e.getValue().weight,
            (v1, v2) -> v1,
            TreeMap::new
        ));
  }

  // -------------------------------------------------------------------------------------------------------------------
  // "Iterator" like helpers (forEach, collect), mostly intended for testing purposes
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Iterates over the results of this data aggregation
   *
   * This method can be handy for testing purposes. But note that since the `action` doesn't produce a return value, it must facilitate its own way of producing output.
   *
   * @param action function that gets called for each transformed data entry
   * @throws Exception
   */
  @Deprecated
  public void forEach(SerializableBiConsumer<U, List<X>> action) throws Exception {
    this.collect().forEach(action);
  }

  /**
   * Collects the results of this data aggregation into Lists
   *
   * @return an aggregated map of lists with all results
   * @throws Exception
   */
  public SortedMap<U, List<X>> collect() throws Exception {
    return this.reduce(
        LinkedList::new,
        (acc, cur) -> { acc.add(cur); return acc; },
        (list1, list2) -> { LinkedList<X> combinedLists = new LinkedList<>(list1); combinedLists.addAll(list2); return combinedLists; }
    );
  }

  // -------------------------------------------------------------------------------------------------------------------
  // "map", "flatMap" transformation methods
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Set an arbitrary `map` transformation function.
   *
   * @param mapper function that will be applied to each data entry (osm entity snapshot or contribution)
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return the MapAggregator object operating on the transformed type (&lt;R&gt;)
   */
  public <R> MapAggregator<U, R> map(SerializableFunction<X, R> mapper) {
    this._mapReducer.map(data -> {
      data.setValue((X)mapper.apply(data.getValue()));
      return data;
    });
    return (MapAggregator<U, R>)this;
  }

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with an arbitrary number of results per input data entry.
   * The results of this function will be "flattened", meaning that they can be for example transformed again by setting additional `map` functions.
   *
   * @param flatMapper function that will be applied to each data entry (osm entity snapshot or contribution) and returns a list of results
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return the MapAggregator object operating on the transformed type (&lt;R&gt;)
   */
  public <R> MapAggregator<U, R> flatMap(SerializableFunction<X, List<R>> flatMapper) {
    this._mapReducer.flatMap(data -> {
      List<Pair<U, R>> results = new LinkedList<>();
      flatMapper.apply(data.getValue()).forEach(flatMappedData ->
          results.add(new MutablePair<U, R>(
              data.getKey(),
              flatMappedData
          ))
      );
      return results;
    });
    return (MapAggregator<U, R>)this;
  }

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param f the filter function that determines if the respective data should be passed on (when f returns true) or discarded (when f returns false)
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapAggregator<U, X> filter(SerializablePredicate<X> f) {
    this._mapReducer.filter(data -> f.test(data.getValue()));
    return this;
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Exposed generic reduce.
  // Can be used by experienced users of the api to implement complex queries.
  // These offer full flexibility, but are potentially a bit tricky to work with (see javadoc).
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Map-reduce routine with built-in aggregation
   *
   * This can be used to perform an arbitrary reduce routine whose results are aggregated separately according to some custom index value.
   *
   * The combination of the used types and identity/reducer functions must make "mathematical" sense:
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function: `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a> interface.
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;) and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter (mutate) the state of the accumulation value (e.g. directly adding new values to an existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function must be pure (have no side effects), and is not allowed to alter the state of the two input objects it gets!</b>
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the `combiner` function, after all `mapper` results have been aggregated (in the `accumulator` and `combiner` steps)
   * @throws Exception
   */
  public <S> SortedMap<U, S> reduce(SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    return this._mapReducer.reduce(
        TreeMap::new,
        (TreeMap<U, S> m, Pair<U, X> r) -> {
          m.put(r.getKey(), accumulator.apply(
              m.getOrDefault(r.getKey(), identitySupplier.get()),
              r.getValue()
          ));
          return m;
        },
        (a,b) -> {
          TreeMap<U, S> combined = new TreeMap<U, S>(a);
          for (SortedMap.Entry<U, S> entry: b.entrySet()) {
            combined.merge(entry.getKey(), entry.getValue(), combiner);
          }
          return combined;
        }
    );
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Some helper methods for internal use in the mapReduce functions
  // -------------------------------------------------------------------------------------------------------------------

  // casts current results to a numeric type, for summing and averaging
  private MapAggregator<U, Number> makeNumeric() {
    return this.map(x -> {
      if (!Number.class.isInstance(x)) // todo: slow??
        throw new UnsupportedOperationException("Cannot convert to non-numeric values of type: " + x.getClass().toString());
      return (Number)x;
    });
  }
}
