package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.tdunning.math.stats.TDigest;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.api.generic.NumberUtils;
import org.heigit.bigspatialdata.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.bigspatialdata.oshdb.api.generic.WeightedValue;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiConsumer;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializablePredicate;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer.Grouping;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.ohsome.filter.FilterExpression;
import org.jetbrains.annotations.Contract;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;


/**
 * A MapReducer with built-in aggregation by an arbitrary index.
 *
 * <p>This class provides similar functionality as a MapReducer, with the difference that here the
 * `reduce` does automatic aggregation of results by the values returned by an arbitrary indexing
 * function.</p>
 *
 * <p>All results for which the set `indexer` returns the same value are aggregated into separate
 * "bins". This can be used to aggregate results by timestamp, geographic region, user id, osm tag,
 * etc.</p>
 *
 * <p>Internally, this wraps around an existing MapReducer object, which still continues to be
 * responsible for all actual calculations.</p>
 *
 * @param <X> the type that is returned by the currently set of mapper function. the next added
 *            mapper function will be called with a parameter of this type as input
 * @param <U> the type of the index values returned by the `mapper function`, used to group results
 */
public class MapAggregator<U extends Comparable<U> & Serializable, X> implements
    Mappable<X>, MapReducerSettings<MapAggregator<U, X>>, MapReducerAggregations<X> {
  private MapReducer<IndexValuePair<U, X>> mapReducer;
  private final List<Collection<? extends Comparable>> zerofill;

  /**
   * Basic constructor.
   *
   * @param mapReducer mapReducer object which will be doing all actual calculations
   * @param indexer function that returns the index value into which to aggregate the respective
   *        result
   * @param zerofill collection of index values that should always be present in the final result
   *        (also if they don't appear in the requested data)
   */
  MapAggregator(
      MapReducer<X> mapReducer,
      SerializableFunction<X, U> indexer,
      Collection<U> zerofill
  ) {
    this.mapReducer = mapReducer.map(data -> new IndexValuePair<U, X>(
        indexer.apply(data),
        data
    ));
    this.zerofill = new ArrayList<>(1);
    this.zerofill.add(zerofill);
  }

  // "copy/transform" constructor
  private MapAggregator(MapAggregator<U, ?> obj, MapReducer<IndexValuePair<U, X>> mapReducer) {
    this.mapReducer = mapReducer;
    this.zerofill = new ArrayList<>(obj.zerofill);
  }

  /**
   * Creates new mapAggregator object for a specific mapReducer that already contains an
   * aggregation index.
   *
   * <p>Used internally for returning type safe copies of the current mapAggregator object after
   * map/flatMap/filter operations.</p>
   *
   * @param mapReducer a special mapReducer for use in map-aggregate operations
   * @param <R> type of data to be "mapped"
   * @return the mapAggregator object using the given mapReducer
   */
  @Contract(pure = true)
  private <R> MapAggregator<U, R> copyTransform(MapReducer<IndexValuePair<U, R>> mapReducer) {
    return new MapAggregator<>(this, mapReducer);
  }

  @Contract(pure = true)
  private <V extends Comparable<V> & Serializable> MapAggregator<V, X>
      copyTransformKey(MapReducer<IndexValuePair<V, X>> mapReducer) {
    @SuppressWarnings("unchecked") // we convert the mapAggregator to a new key type "V"
    MapAggregator<V, ?> transformedMapAggregator = (MapAggregator<V, ?>) this;
    return new MapAggregator<V, X>(transformedMapAggregator, mapReducer);
  }

  // -----------------------------------------------------------------------------------------------
  // MapAggregator specific methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Sets up aggregation by another custom index.
   *
   * @param indexer a callback function that returns an index object for each given data
   * @param zerofill a collection of values that are expected to be present in the result
   * @return a MapAggregatorByIndex object with the new index applied as well
   */
  @Contract(pure = true)
  public <V extends Comparable<V> & Serializable> MapAggregator<OSHDBCombinedIndex<U, V>, X>
      aggregateBy(SerializableFunction<X, V> indexer, Collection<V> zerofill) {
    MapAggregator<OSHDBCombinedIndex<U, V>, X> res = this
        .mapIndex((existingIndex, data) -> new OSHDBCombinedIndex<U, V>(
            existingIndex,
            indexer.apply(data)
        ));
    res.zerofill.add(zerofill);
    return res;
  }

  /**
   * Sets up aggregation by another custom index.
   *
   * @param indexer a callback function that returns an index object for each given data.
   * @return a MapAggregatorByIndex object with the new index applied as well
   */
  @Contract(pure = true)
  public <V extends Comparable<V> & Serializable> MapAggregator<OSHDBCombinedIndex<U, V>, X>
      aggregateBy(SerializableFunction<X, V> indexer) {
    return this.aggregateBy(indexer, Collections.emptyList());
  }

  /**
   * Sets up aggregation by a custom time index.
   *
   * <p>The timestamps returned by the supplied indexing function are matched to the corresponding
   * time intervals</p>
   *
   * @param indexer a callback function that returns a timestamp object for each given data.
   *                Note that if this function returns timestamps outside of the supplied
   *                timestamps() interval results may be undefined
   * @return a MapAggregatorByTimestampAndIndex object with the equivalent state (settings,
   *         filters, map function, etc.) of the current MapReducer object
   */
  @Contract(pure = true)
  public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer
  ) {
    final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this.mapReducer.tstamps.get());
    final OSHDBTimestamp minTime = timestamps.first();
    final OSHDBTimestamp maxTime = timestamps.last();
    return this.aggregateBy(data -> {
      // match timestamps to the given timestamp list
      OSHDBTimestamp aggregationTimestamp = indexer.apply(data);
      if (aggregationTimestamp == null
          || aggregationTimestamp.compareTo(minTime) < 0
          || aggregationTimestamp.compareTo(maxTime) > 0) {
        throw new OSHDBInvalidTimestampException(
            "Aggregation timestamp outside of time query interval."
        );
      }
      return timestamps.floor(aggregationTimestamp);
    }, this.mapReducer.getZerofillTimestamps());
  }
  
  /**
   * Aggregates the results by sub-regions as well, in addition to the timestamps.
   *
   * <p>Cannot be used together with the `groupByEntity()` setting enabled.</p>
   *
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has
   *         been activated
   * @throws UnsupportedOperationException when called after any map or flatMap functions are set
   */
  @Contract(pure = true)
  public <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal>
      MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateByGeometry(Map<V, P> geometries)
      throws UnsupportedOperationException {
    if (this.mapReducer.grouping != Grouping.NONE) {
      throw new UnsupportedOperationException(
          "aggregateByGeometry() cannot be used together with the groupByEntity() functionality"
      );
    }

    GeometrySplitter<V> gs = new GeometrySplitter<>(geometries);
    if (this.mapReducer.mappers.size() > 1) {
      // todo: fix
      throw new UnsupportedOperationException(
          "please call aggregateByGeometry before setting any map or flatMap functions"
      );
    } else {
      MapAggregator<OSHDBCombinedIndex<U, V>, ? extends OSHDBMapReducible> ret;
      if (this.mapReducer.forClass.equals(OSMContribution.class)) {
        ret = this.flatMap(x -> gs.splitOSMContribution((OSMContribution) x).entrySet())
            .aggregateBy(Entry::getKey, geometries.keySet()).map(Entry::getValue);
      } else if (this.mapReducer.forClass.equals(OSMEntitySnapshot.class)) {
        ret = this.flatMap(x -> gs.splitOSMEntitySnapshot((OSMEntitySnapshot) x).entrySet())
            .aggregateBy(Entry::getKey, geometries.keySet()).map(Entry::getValue);
      } else {
        throw new UnsupportedOperationException(
            "aggregateByGeometry not implemented for objects of type: "
                + this.mapReducer.forClass.toString()
        );
      }
      @SuppressWarnings("unchecked") // no mapper functions have been applied -> the type is still X
      MapAggregator<OSHDBCombinedIndex<U, V>, X> result =
          (MapAggregator<OSHDBCombinedIndex<U, V>, X>) ret;
      return result;
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Filtering methods
  // Just forwards everything to the wrapped MapReducer object
  // -----------------------------------------------------------------------------------------------

  /**
   * Set the area of interest to the given bounding box.
   *
   * <p>Only objects inside or clipped by this bbox will be passed on to the analysis' `mapper`
   * function.</p>
   *
   * @param bboxFilter the bounding box to query the data in
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> areaOfInterest(OSHDBBoundingBox bboxFilter) {
    return this.copyTransform(this.mapReducer.areaOfInterest(bboxFilter));
  }

  /**
   * Set the area of interest to the given polygon.
   * Only objects inside or clipped by this polygon will be passed on to the analysis'
   * `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public <P extends Geometry & Polygonal> MapAggregator<U, X> areaOfInterest(P polygonFilter) {
    return this.copyTransform(this.mapReducer.areaOfInterest(polygonFilter));
  }

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param typeFilter the set of osm types to filter (e.g. `EnumSet.of(OSMType.WAY)`)
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmType(Set<OSMType> typeFilter) {
    return this.copyTransform(this.mapReducer.osmType(typeFilter));
  }

  /**
   * Adds a custom arbitrary filter that gets executed for each osm entity and determines if it
   * should be considered for this analyis or not.
   *
   * @param f the filter function to call for each osm entity
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmEntityFilter(SerializablePredicate<OSMEntity> f) {
    return this.copyTransform(this.mapReducer.osmEntityFilter(f));
  }


  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * (with an arbitrary value), or this tag key and value.
   *
   * @param tag the tag (key, or key and value) to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(OSMTagInterface tag) {
    return this.copyTransform(this.mapReducer.osmTag(tag));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * (with an arbitrary value).
   *
   * @param key the tag key to filter the osm entities for
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(String key) {
    return this.copyTransform(this.mapReducer.osmTag(key));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * and value.
   *
   * @param key the tag key to filter the osm entities for
   * @param value the tag value to filter the osm entities for
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(String key, String value) {
    return this.copyTransform(this.mapReducer.osmTag(key, value));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * and one of the
   * given values.
   *
   * @param key the tag key to filter the osm entities for
   * @param values an array of tag values to filter the osm entities for
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(String key, Collection<String> values) {
    return this.copyTransform(this.mapReducer.osmTag(key, values));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have a tag with
   * the given key and whose value matches the given regular expression pattern.
   *
   * @param key the tag key to filter the osm entities for
   * @param valuePattern a regular expression which the tag value of the osm entity must match
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(String key, Pattern valuePattern) {
    return this.copyTransform(this.mapReducer.osmTag(key, valuePattern));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have at least one
   * of the supplied tags (key=value pairs or key=*).
   *
   * @param tags the tags (key/value pairs or key=*) to filter the osm entities for
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> osmTag(Collection<? extends OSMTagInterface> tags) {
    return this.copyTransform(this.mapReducer.osmTag(tags));
  }

  // -----------------------------------------------------------------------------------------------
  // "Quality of life" helper methods to use the map-reduce functionality more directly and easily
  // for typical queries.
  // Available are: sum, count, average, weightedAverage and uniq.
  // Each one can be used to get results aggregated by timestamp, aggregated by a custom index and
  // not aggregated totals.
  // -----------------------------------------------------------------------------------------------

  /**
   * Sums up the results.
   *
   * <p>The current data values need to be numeric (castable to "Number" type), otherwise a
   * runtime exception will be thrown.</p>
   *
   * @return the sum of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   */
  @Contract(pure = true)
  public SortedMap<U, Number> sum() throws Exception {
    return this
        .makeNumeric()
        .reduce(
            () -> 0,
            NumberUtils::add
        );
  }

  /**
   * Sums up the results provided by a given `mapper` function.
   *
   * <p>This is a shorthand for `.map(mapper).sum()`, with the difference that here the numerical
   * return type of the `mapper` is ensured.</p>
   *
   * @param mapper function that returns the numbers to sum up
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the summed up results of the `mapper` function
   */
  @Contract(pure = true)
  public <R extends Number> SortedMap<U, R> sum(SerializableFunction<X, R> mapper)
      throws Exception {
    return this
        .map(mapper)
        .reduce(
            () -> (R) (Integer) 0,
            NumberUtils::add
        );
  }

  /**
   * Counts the number of results.
   *
   * @return the total count of features or modifications, summed up over all timestamps
   */
  @Contract(pure = true)
  public SortedMap<U, Integer> count() throws Exception {
    return this.sum(ignored -> 1);
  }

  /**
   * Gets all unique values of the results.
   *
   * <p>For example, this can be used together with the OSMContributionView to get the total
   * amount of unique users editing specific feature types.</p>
   *
   * @return the set of distinct values
   */
  @Contract(pure = true)
  public SortedMap<U, Set<X>> uniq() throws Exception {
    return this.reduce(
        MapReducer::uniqIdentitySupplier,
        MapReducer::uniqAccumulator,
        MapReducer::uniqCombiner
    );
  }

  /**
   * Gets all unique values of the results provided by a given mapper function.
   *
   * <p>This is a shorthand for `.map(mapper).uniq()`.</p>
   *
   * @param mapper function that returns some values
   * @param <R> the type that is returned by the `mapper` function
   * @return a set of distinct values returned by the `mapper` function
   */
  @Contract(pure = true)
  public <R> SortedMap<U, Set<R>> uniq(SerializableFunction<X, R> mapper) throws Exception {
    return this.map(mapper).uniq();
  }

  /**
   * Counts all unique values of the results.
   *
   * <p>For example, this can be used together with the OSMContributionView to get the number of
   * unique users editing specific feature types.</p>
   *
   * @return the set of distinct values
   */
  @Contract(pure = true)
  public SortedMap<U, Integer> countUniq() throws Exception {
    return transformSortedMap(this.uniq(), Set::size);
  }

  /**
   * Calculates the averages of the results.
   *
   * <p>The current data values need to be numeric (castable to "Number" type), otherwise a runtime
   * exception will be thrown.</p>
   *
   * @return the average of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   */
  @Contract(pure = true)
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
   */
  @Contract(pure = true)
  public <R extends Number> SortedMap<U, Double> average(SerializableFunction<X, R> mapper)
      throws Exception {
    return this.weightedAverage(data -> new WeightedValue(mapper.apply(data), 1.0));
  }

  /**
   * Calculates the weighted average of the results provided by the `mapper` function.
   *
   * <p>The mapper must return an object of the type `WeightedValue` which contains a numeric
   * value associated with a (floating point) weight.</p>
   *
   * @param mapper function that gets called for each entity snapshot or modification, needs to
   *        return the value and weight combination of numbers to average
   * @return the weighted average of the numbers returned by the `mapper` function
   */
  @Contract(pure = true)
  public SortedMap<U, Double> weightedAverage(SerializableFunction<X, WeightedValue> mapper)
      throws Exception {
    return transformSortedMap(
        this.map(mapper).reduce(
            MutableWeightedDouble::identitySupplier,
            MutableWeightedDouble::accumulator,
            MutableWeightedDouble::combiner
        ),
        x -> x.num / x.weight
    );
  }

  /**
   * Returns an estimate of the median of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @return estimated median
   */
  @Contract(pure = true)
  public SortedMap<U, Double> estimatedMedian() throws Exception {
    return this.estimatedQuantile(0.5);
  }

  /**
   * Returns an estimate of the median of the results after applying the given map function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the mean for
   * @return estimated median
   */
  @Contract(pure = true)
  public <R extends Number> SortedMap<U, Double> estimatedMedian(SerializableFunction<X, R> mapper)
      throws Exception {
    return this.estimatedQuantile(mapper, 0.5);
  }

  /**
   * Returns an estimate of a requested quantile of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param q the desired quantile to calculate (as a number between 0 and 1)
   * @return estimated quantile boundary
   */
  @Contract(pure = true)
  public SortedMap<U, Double> estimatedQuantile(double q) throws Exception {
    return this.makeNumeric().estimatedQuantile(n -> n, q);
  }

  /**
   * Returns an estimate of a requested quantile of the results after applying the given map
   * function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the quantile for
   * @param q the desired quantile to calculate (as a number between 0 and 1)
   * @return estimated quantile boundary
   */
  @Contract(pure = true)
  public <R extends Number> SortedMap<U, Double> estimatedQuantile(
      SerializableFunction<X, R> mapper,
      double q
  ) throws Exception {
    return transformSortedMap(
        this.estimatedQuantiles(mapper),
        quantileFunction -> quantileFunction.applyAsDouble(q)
    );
  }

  /**
   * Returns an estimate of the quantiles of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param q the desired quantiles to calculate (as a collection of numbers between 0 and 1)
   * @return estimated quantile boundaries
   */
  @Contract(pure = true)
  public SortedMap<U, List<Double>> estimatedQuantiles(Iterable<Double> q) throws Exception {
    return this.makeNumeric().estimatedQuantiles(n -> n, q);
  }

  /**
   * Returns an estimate of the quantiles of the results after applying the given map function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the quantiles for
   * @param q the desired quantiles to calculate (as a collection of numbers between 0 and 1)
   * @return estimated quantile boundaries
   */
  @Contract(pure = true)
  public <R extends Number> SortedMap<U, List<Double>> estimatedQuantiles(
      SerializableFunction<X, R> mapper,
      Iterable<Double> q
  ) throws Exception {
    return transformSortedMap(
        this.estimatedQuantiles(mapper),
        quantileFunction -> Streams.stream(q)
            .mapToDouble(Double::doubleValue)
            .map(quantileFunction)
            .boxed()
            .collect(Collectors.toList())
    );
  }

  /**
   * Returns a function that computes estimates of arbitrary quantiles of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @return a function that computes estimated quantile boundaries
   */
  @Contract(pure = true)
  public SortedMap<U, DoubleUnaryOperator> estimatedQuantiles() throws Exception {
    return this.makeNumeric().estimatedQuantiles(n -> n);
  }

  /**
   * Returns a function that computes estimates of arbitrary quantiles of the results after applying
   * the given map function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the quantiles for
   * @return a function that computes estimated quantile boundaries
   */
  @Contract(pure = true)
  public <R extends Number> SortedMap<U, DoubleUnaryOperator> estimatedQuantiles(
      SerializableFunction<X, R> mapper
  ) throws Exception {
    return transformSortedMap(this.digest(mapper), d -> d::quantile);
  }

  /**
   * Generates the t-digest of the complete result set. see:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   */
  @Contract(pure = true)
  private <R extends Number> SortedMap<U, TDigest> digest(SerializableFunction<X, R> mapper)
      throws Exception {
    return this.map(mapper).reduce(
        TDigestReducer::identitySupplier,
        TDigestReducer::accumulator,
        TDigestReducer::combiner
    );
  }

  // -----------------------------------------------------------------------------------------------
  // "Iterator" like helpers (forEach, collect), mostly intended for testing purposes
  // -----------------------------------------------------------------------------------------------

  /**
   * Iterates over the results of this data aggregation.
   *
   * <p>This method can be handy for testing purposes. But note that since the `action` doesn't
   * produce a return value, it must facilitate its own way of producing output.</p>
   *
   * <p>If you'd like to use such a "forEach" in a non-test use case, use `.collect().forEach()` or
   * `.stream().forEach()`  instead.</p>
   *
   * @param action function that gets called for each transformed data entry
   * @deprecated only for testing purposes. use `.collect().forEach()` or `.stream().forEach()`
   *             instead
   */
  @Deprecated
  public void forEach(SerializableBiConsumer<U, List<X>> action) throws Exception {
    this.collect().forEach(action);
  }

  /**
   * Collects the results of this data aggregation into Lists.
   *
   * @return an aggregated map of lists with all results
   */
  @Contract(pure = true)
  public SortedMap<U, List<X>> collect() throws Exception {
    return this.reduce(
        MapReducer::collectIdentitySupplier,
        MapReducer::collectAccumulator,
        MapReducer::collectCombiner
    );
  }

  /**
   * Returns all results as a Stream.
   *
   * @return a stream with all results returned by the `mapper` function
   */
  @Contract(pure = true)
  public Stream<Entry<U, X>> stream() throws Exception {
    return this.mapReducer.stream().map(d -> new Entry<U, X>() {
      @Override
      public U getKey() {
        return d.getKey();
      }

      @Override
      public X getValue() {
        return d.getValue();
      }

      @Override
      public X setValue(X value) {
        throw new RuntimeException("cannot modify the value of this entry");
      }
    });
  }

  // -----------------------------------------------------------------------------------------------
  // "map", "flatMap" transformation methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Set an arbitrary `map` transformation function.
   *
   * @param mapper function that will be applied to each data entry (osm entity snapshot or
   *        contribution)
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of this MapAggregator object operating on the transformed type R
   */
  @Contract(pure = true)
  public <R> MapAggregator<U, R> map(SerializableFunction<X, R> mapper) {
    return this.copyTransform(this.mapReducer.map(inData -> {
      @SuppressWarnings("unchecked")
      // trick/hack to replace mapped values without copying pair objects
      IndexValuePair<U,R> outData = (IndexValuePair<U,R>)inData;
      outData.setValue(mapper.apply(inData.getValue()));
      return outData;
    }));
  }

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with an arbitrary number
   * of results per input data entry.
   *
   * <p>The results of this function will be "flattened", meaning that they can be for example
   * transformed again by setting additional `map` functions.</p>
   *
   * @param flatMapper function that will be applied to each data entry (osm entity snapshot or
   *        contribution) and returns a list of results
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of this MapAggregator object operating on the transformed type R
   */
  @Contract(pure = true)
  public <R> MapAggregator<U, R> flatMap(SerializableFunction<X, Iterable<R>> flatMapper) {
    return this.copyTransform(this.mapReducer.flatMap(inData -> {
      List<IndexValuePair<U, R>> outData = new LinkedList<>();
      flatMapper.apply(inData.getValue()).forEach(flatMappedData ->
          outData.add(new IndexValuePair<U, R>(
              inData.getKey(),
              flatMappedData
          ))
      );
      return outData;
    }));
  }

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param f the filter function that determines if the respective data should be passed on
   *        (when f returns true) or discarded (when f returns false)
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> filter(SerializablePredicate<X> f) {
    return this.copyTransform(this.mapReducer.filter(data ->
      f.test(data.getValue())
    ));
  }

  /**
   * Apply a custom "ohsome" filter expression to this query.
   *
   * <p>See https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/libs/ohsome-filter#readme
   * and https://docs.ohsome.org/java/ohsome-filter/1.2-SNAPSHOT for further information about how
   * to create such a filter expression object.</p>
   *
   * @param f the filter expression to apply to the mapAggregator
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> filter(FilterExpression f) {
    return this.copyTransform(this.mapReducer.filter(f));
  }

  /**
   * Apply a custom "ohsome" filter to this query.
   *
   * <p>See https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/libs/ohsome-filter#syntax
   * for a description of the ohsome filter syntax.</p>
   *
   * @param f the ohsome filter string to apply to the mapAggregator
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregator<U, X> filter(String f) {
    return this.copyTransform(this.mapReducer.filter(f));
  }

  // -----------------------------------------------------------------------------------------------
  // Exposed generic reduce.
  // Can be used by experienced users of the api to implement complex queries.
  // These offer full flexibility, but are potentially a bit tricky to work with (see javadoc).
  // -----------------------------------------------------------------------------------------------

  /**
   * Map-reduce routine with built-in aggregation.
   *
   * <p>
   * This can be used to perform an arbitrary reduce routine whose results are aggregated separately
   * according to some custom index value.
   * </p>
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner
   *   function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function:
   *   `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java8 Stream's <a
   * href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`)
   *        and returns the "sum" of the two; contrary to `combiner`, this function is allowed to
   *        alter (mutate) the state of the accumulation value (e.g. directly adding new values to
   *        an existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  @Contract(pure = true)
  public <S> SortedMap<U, S> reduce(
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator,
      SerializableBinaryOperator<S> combiner)
      throws Exception {
    SortedMap<U, S> result = this.mapReducer.reduce(
        TreeMap::new,
        (TreeMap<U, S> m, IndexValuePair<U, X> r) -> {
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
    // fill nodata entries with "0"
    @SuppressWarnings("unchecked") // all zerofills must "add up" to <U>
    Collection<U> zerofill = (Collection<U>) this.completeZerofill(
        result.keySet(),
        Lists.reverse(this.zerofill)
    );
    zerofill.forEach(zerofillKey -> {
      if (!result.containsKey(zerofillKey)) {
        result.put(zerofillKey, identitySupplier.get());
      }
    });
    return result;
  }

  /**
   * Map-reduce routine with built-in aggregation (shorthand syntax).
   * <p>
   * This can be used to perform an arbitrary reduce routine whose results are aggregated
   * separately according to some custom index value.
   * </p>
   *
   * <p>
   * This variant is shorter to program than `reduce(identitySupplier, accumulator, combiner)`,
   * but can only be used if the result type is the same as the current `map`ped type &lt;X&gt;.
   * Also this variant can be less efficient since it cannot benefit from the mutability freedoms
   * the accumulator+combiner approach has.
   * </p>
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner
   *   function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function:
   *   `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java8 Stream's <a
   * href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;X&gt;)
   *        and an accumulation value (also of type &lt;X&gt;, e.g. the result of
   *        `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this
   *        function is not to alter (mutate) the state of the accumulation value (e.g. directly
   *        adding new values to an existing Set object)
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  @Contract(pure = true)
  public SortedMap<U, X> reduce(
      SerializableSupplier<X> identitySupplier,
      SerializableBinaryOperator<X> accumulator
  ) throws Exception {
    return this.reduce(identitySupplier, accumulator::apply, accumulator);
  }

  // -----------------------------------------------------------------------------------------------
  // Some helper methods for internal use in the mapReduce functions
  // -----------------------------------------------------------------------------------------------

  // casts current results to a numeric type, for summing and averaging
  @Contract(pure = true)
  private MapAggregator<U, Number> makeNumeric() {
    return this.map(MapReducer::checkAndMapToNumeric);
  }

  // maps from one index type to a different one
  @Contract(pure = true)
  private <V extends Comparable<V> & Serializable> MapAggregator<V, X> mapIndex(
      SerializableBiFunction<U, X, V> keyMapper) {
    return this.copyTransformKey(this.mapReducer.map(inData -> new IndexValuePair<>(
        keyMapper.apply(inData.getKey(), inData.getValue()),
        inData.getValue()
    )));
  }

  // calculate complete set of indices to use for zerofilling
  @SuppressWarnings("rawtypes")
  // called recursively: the exact types of the zerofills are not known at this point
  private Collection<? extends Comparable> completeZerofill(
      Set<? extends Comparable> keys,
      List<Collection<? extends Comparable>> zerofills
  ) {
    if (zerofills.isEmpty()) {
      return Collections.emptyList();
    }
    SortedSet<Comparable> seen = new TreeSet<>(zerofills.get(0));
    SortedSet<Comparable> nextLevelKeys = new TreeSet<>();
    for (Comparable index : keys) {
      Comparable v;
      if (index instanceof OSHDBCombinedIndex) {
        v = ((OSHDBCombinedIndex) index).getSecondIndex();
        nextLevelKeys.add(((OSHDBCombinedIndex) index).getFirstIndex());
      } else {
        v = index;
      }
      seen.add(v);
    }
    if (zerofills.size() == 1) {
      return seen;
    } else {
      Collection<? extends Comparable> nextLevel = this.completeZerofill(
          nextLevelKeys,
          zerofills.subList(1, zerofills.size())
      );
      @SuppressWarnings("unchecked") // we don't know the exact types of u and v at this point
      Stream<OSHDBCombinedIndex> combinedZerofillIndices = nextLevel.stream().flatMap(u ->
          seen.stream().map(v -> new OSHDBCombinedIndex(u, v))
      );
      return combinedZerofillIndices.collect(Collectors.toList());
    }
  }

  // transforms the values of a sorted map by a given function (similar to Stream::map)
  private <A, B> SortedMap<U, B> transformSortedMap(SortedMap<U, A> in, Function<A, B> transform) {
    return in.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey,
        e -> transform.apply(e.getValue()),
        (v1, v2) -> {
          assert false;
          return v1;
        },
        TreeMap::new
    ));
  }
  
  /**
   * A generic Pair class for holding index/value pairs.
   */
  private static class IndexValuePair<U, X> {
    private U key;
    protected X value;

    private IndexValuePair(U key, X value) {
      this.key = key;
      this.value = value;
    }

    public U getKey() {
      return key;
    }

    public X getValue() {
      return value;
    }

    public void setValue(X value) {
      this.value = value;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }

    @Override
    public boolean equals(Object other) {
      return this == other || other instanceof MapAggregator.IndexValuePair
          && Objects.equals(key, ((IndexValuePair) other).key)
          && Objects.equals(value, ((IndexValuePair) other).value);
    }

    @Override
    public String toString() {
      return "[index=" + key + ", value=" + value + "]";
    }
  }
}
