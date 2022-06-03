package org.heigit.ohsome.oshdb.api.mapreducer.base;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDB;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.filter.AndOperator;
import org.heigit.ohsome.oshdb.filter.Filter;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
import org.heigit.ohsome.oshdb.filter.FilterParser;
import org.heigit.ohsome.oshdb.filter.GeometryTypeFilter;
import org.heigit.ohsome.oshdb.filter.TagFilterEquals;
import org.heigit.ohsome.oshdb.filter.TagFilterEqualsAny;
import org.heigit.ohsome.oshdb.filter.TypeFilter;
import org.heigit.ohsome.oshdb.index.XYGridTree;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.ohsome.oshdb.util.function.OSHEntityFilter;
import org.heigit.ohsome.oshdb.util.function.OSMEntityFilter;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.heigit.ohsome.oshdb.util.geometry.Geo;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.taginterpreter.DefaultTagInterpreter;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestampList;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class of oshdb's "functional programming" API.
 *
 * <p>It accepts a list of filters, transformation `map` functions a produces a result when calling
 * the `reduce` method (or one of its shorthand versions like `sum`, `count`, etc.).</p>
 *
 * <p>
 * You can set a list of filters that are applied on the raw OSM data, for example you can filter:
 * </p>
 * <ul>
 * <li>geometrically by an area of interest (bbox or polygon)</li>
 * <li>by osm tags (key only or key/value)</li>
 * <li>by OSM type</li>
 * <li>custom filter callback</li>
 * </ul>
 *
 * <p>Depending on the used data "view", the MapReducer produces either "snapshots" or evaluated
 * all modifications ("contributions") of the matching raw OSM data.</p>
 *
 * <p>These data can then be transformed arbitrarily by user defined `map` functions (which take one
 * of these entity snapshots or modifications as input an produce an arbitrary output) or `flatMap`
 * functions (which can return an arbitrary number of results per entity snapshot/contribution). It
 * is possible to chain together any number of transformation functions.</p>
 *
 * <p>Finally, one can either use one of the pre-defined result-generating functions (e.g. `sum`,
 * `count`, `average`, `uniq`), or specify a custom `reduce` procedure.</p>
 *
 * <p>If one wants to get results that are aggregated by timestamp (or some other index), one can
 * use the `aggregateByTimestamp` or `aggregateBy` functionality that automatically handles the
 * grouping of the output data.</p>
 *
 * <p>For more complex analyses, it is also possible to enable the grouping of the input data by
 * the respective OSM ID. This can be used to view at the whole history of entities at once.</p>
 *
 * @param <X> the type that is returned by the currently set of mapper function. the next added
 *        mapper function will be called with a parameter of this type as input
 */
public abstract class MapReducerBase<X> implements
        MapReducer<X> {

  private static final Logger LOG = LoggerFactory.getLogger(MapReducerBase.class);
  protected static final String TAG_KEY_NOT_FOUND =
      "Tag key {} not found. No data will match this filter.";
  protected static final String TAG_NOT_FOUND =
      "Tag {}={} not found. No data will match this filter.";
  protected static final String EMPTY_TAG_LIST =
      "Empty tag value list. No data will match this filter.";
  protected static final String UNIMPLEMENTED_DATA_VIEW = "Unimplemented data view: %s";
  protected static final String UNSUPPORTED_GROUPING = "Unsupported grouping: %s";

  protected transient OSHDBDatabase oshdb;
  protected transient OSHDBJdbc keytables;

  protected Long timeout = null;

  /** the class representing the used OSHDB view: either {@link OSMContribution} or
   * {@link OSMEntitySnapshot}. */
  Class<? extends OSHDBMapReducible> viewClass;

  enum Grouping {
    NONE, BY_ID
  }

  Grouping grouping = Grouping.NONE;

  // utility objects
  private transient TagTranslator tagTranslator = null;
  private TagInterpreter tagInterpreter = null;

  // settings and filters
  protected OSHDBTimestampList tstamps = new OSHDBTimestamps(
      "2008-01-01",
      currentDate(),
      OSHDBTimestamps.Interval.MONTHLY
      );
  protected OSHDBBoundingBox bboxFilter = bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0);
  private Geometry polyFilter = null;
  protected EnumSet<OSMType> typeFilter = EnumSet.of(OSMType.NODE, OSMType.WAY, OSMType.RELATION);
  private final List<SerializablePredicate<OSHEntity>> preFilters = new ArrayList<>();
  private final List<SerializablePredicate<OSMEntity>> filters = new ArrayList<>();
  final LinkedList<MapFunction> mappers = new LinkedList<>();

  // basic constructor
  protected MapReducerBase(OSHDBDatabase oshdb, Class<? extends OSHDBMapReducible> viewClass) {
    this.oshdb = oshdb;
    this.viewClass = viewClass;
  }

  // copy constructor
  protected MapReducerBase(MapReducerBase<?> obj) {
    this.oshdb = obj.oshdb;
    this.keytables = obj.keytables;

    this.viewClass = obj.viewClass;
    this.grouping = obj.grouping;

    this.tagTranslator = obj.tagTranslator;
    this.tagInterpreter = obj.tagInterpreter;

    this.tstamps = obj.tstamps;
    this.bboxFilter = obj.bboxFilter;
    this.polyFilter = obj.polyFilter;
    this.typeFilter = obj.typeFilter.clone();
    this.preFilters.addAll(obj.preFilters);
    this.filters.addAll(obj.filters);
    this.mappers.addAll(obj.mappers);
  }

  @NotNull
  protected abstract MapReducerBase<X> copy();

  // -----------------------------------------------------------------------------------------------
  // "Setting" methods and associated internal helpers
  // -----------------------------------------------------------------------------------------------

  /**
   * Sets the keytables database to use in the calculations to resolve strings (osm tags, roles)
   * into internally used identifiers. If this function is never called, the main database
   * (specified during the construction of this object) is used for this.
   *
   * @param keytables the database to use for resolving strings into internal identifiers
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapReducer<X> keytables(OSHDBJdbc keytables) {
    if (keytables != this.oshdb && this.oshdb instanceof OSHDBJdbc) {
      Connection c = ((OSHDBJdbc) this.oshdb).getConnection();
      boolean oshdbContainsKeytables = true;
      try {
        new TagTranslator(c).close();
      } catch (OSHDBKeytablesNotFoundException e) {
        // this is the expected path -> the oshdb doesn't have the key tables
        oshdbContainsKeytables = false;
      } catch (SQLException e) {
        throw new OSHDBException(e);
      }
      if (oshdbContainsKeytables) {
        LOG.warn("It looks like as if the current OSHDB comes with keytables included. "
            + "Usually this means that you should use this file's keytables "
            + "and should not set the keytables manually.");
      }
    }
    MapReducerBase<X> ret = this.copy();
    ret.keytables = keytables;
    return ret;
  }

  /**
   * Sets the tagInterpreter to use in the analysis. The tagInterpreter is used internally to
   * determine the geometry type of osm entities (e.g. an osm way can become either a LineString or
   * a Polygon, depending on its tags). Normally, this is generated automatically for the user. But
   * for example, if one doesn't want to use the DefaultTagInterpreter, it is possible to use this
   * function to supply their own tagInterpreter.
   *
   * @param tagInterpreter the tagInterpreter object to use in the processing of osm entities
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapReducer<X> tagInterpreter(TagInterpreter tagInterpreter) {
    MapReducerBase<X> ret = this.copy();
    ret.tagInterpreter = tagInterpreter;
    return ret;
  }

  // -----------------------------------------------------------------------------------------------
  // Filtering methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Set the area of interest to the given bounding box. Only objects inside or clipped by this bbox
   * will be passed on to the analysis' `mapper` function.
   *
   * @param bboxFilter the bounding box to query the data in
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapReducerBase<X> areaOfInterest(@NotNull OSHDBBoundingBox bboxFilter) {
    MapReducerBase<X> ret = this.copy();
    if (this.polyFilter == null) {
      ret.bboxFilter = bboxFilter.intersection(bboxFilter);
    } else {
      ret.polyFilter = Geo.clip(ret.polyFilter, bboxFilter);
      ret.bboxFilter = OSHDBGeometryBuilder.boundingBoxOf(ret.polyFilter.getEnvelopeInternal());
    }
    return ret;
  }

  /**
   * Set the area of interest to the given polygon. Only objects inside or clipped by this polygon
   * will be passed on to the analysis' `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public <P extends Geometry & Polygonal> MapReducerBase<X> areaOfInterest(P polygonFilter) {
    MapReducerBase<X> ret = this.copy();
    if (this.polyFilter == null) {
      ret.polyFilter = Geo.clip(polygonFilter, ret.bboxFilter);
    } else {
      ret.polyFilter = Geo.clip(polygonFilter, ret.getPolyFilter());
    }
    ret.bboxFilter = OSHDBGeometryBuilder.boundingBoxOf(ret.polyFilter.getEnvelopeInternal());
    return ret;
  }

  /**
   * Set the timestamps for which to perform the analysis.
   *
   * <p>
   * Depending on the *View*, this has slightly different semantics:
   * </p>
   * <ul><li>
   * For the OSMEntitySnapshotView it will set the time slices at which to take the "snapshots"
   * </li><li>
   * For the OSMContributionView it will set the time interval in which to look for
   * osm contributions (only the first and last timestamp of this list are contributing).
   * </li></ul>
   * Additionally, these timestamps are used in the `aggregateByTimestamp` functionality.
   *
   * @param tstamps an object (implementing the OSHDBTimestampList interface) which provides the
   *        timestamps to do the analysis for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapReducer<X> timestamps(OSHDBTimestampList tstamps) {
    MapReducerBase<X> ret = this.copy();
    ret.tstamps = tstamps;
    return ret;
  }

  protected MapReducerBase<X> osmTypeInternal(Set<OSMType> typeFilter) {
    var ret = this.copy();
    typeFilter = Sets.intersection(ret.typeFilter, typeFilter);
    if (typeFilter.isEmpty()) {
      ret.typeFilter = EnumSet.noneOf(OSMType.class);
    } else {
      ret.typeFilter = EnumSet.copyOf(typeFilter);
    }
    return ret;
  }

  @Contract(pure = true)
  private MapReducerBase<X> osmTag(OSHDBTag tag) {
    MapReducerBase<X> ret = this.copy();
    ret.preFilters.add(oshEntity -> oshEntity.hasTagKey(tag.getKey()));
    ret.filters.add(osmEntity -> osmEntity.getTags().hasTagValue(tag.getKey(), tag.getValue()));
    return ret;
  }

  @Contract(pure = true)
  private MapReducerBase<X> osmTag(OSHDBTagKey tagKey) {
    MapReducerBase<X> ret = this.copy();
    ret.preFilters.add(oshEntity -> oshEntity.hasTagKey(tagKey));
    ret.filters.add(osmEntity -> osmEntity.getTags().hasTagKey(tagKey));
    return ret;
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
   * @return a modified copy of this MapReducer object operating on the transformed type (&lt;R&gt;)
   */
  @Override
  @Contract(pure = true)
  public <R> MapReducerBase<R> map(SerializableFunction<X, R> mapper) {
    return map((o, ignored) -> mapper.apply(o));
  }

  // Some internal methods can also map the "root" object of the mapreducer's view.
  @Contract(pure = true)
  protected  <R> MapReducerBase<R> map(SerializableBiFunction<X, Object, R> mapper) {
    MapReducerBase<?> ret = this.copy();
    ret.mappers.add(new MapFunction(mapper, false));
    @SuppressWarnings("unchecked") // after applying this mapper, we have a mapreducer of type R
    MapReducerBase<R> result = (MapReducerBase<R>) ret;
    return result;
  }

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with an arbitrary number
   * of results per input data entry. The results of this function will be "flattened", meaning that
   * they can be for example transformed again by setting additional `map` functions.
   *
   * @param flatMapper function that will be applied to each data entry (osm entity snapshot or
   *        contribution) and returns a list of results
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of this MapReducer object operating on the transformed type (&lt;R&gt;)
   */
  @Override
  @Contract(pure = true)
  public <R> MapReducerBase<R> flatMap(SerializableFunction<X, Iterable<R>> flatMapper) {
    return flatMap((o, ignored) -> flatMapper.apply(o));
  }

  // Some internal methods can also flatMap the "root" object of the mapreducer's view.
  @Contract(pure = true)
  protected <R> MapReducerBase<R> flatMap(
      SerializableBiFunction<X, Object, Iterable<R>> flatMapper) {
    MapReducerBase<?> ret = this.copy();
    ret.mappers.add(new MapFunction(flatMapper, true));
    @SuppressWarnings("unchecked") // after applying this mapper, we have a mapreducer of type R
    MapReducerBase<R> result = (MapReducerBase<R>) ret;
    return result;
  }

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param f the filter function that determines if the respective data should be passed on (when f
   *        returns true) or discarded (when f returns false)
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapReducerBase<X> filter(SerializablePredicate<X> f) {
    return this
        .flatMap(data -> f.test(data) ? Collections.singletonList(data) : Collections.emptyList());
  }

  /**
   * Apply a custom filter expression to this query.
   *
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#readme">oshdb-filter
   *      readme</a> and {@link org.heigit.ohsome.oshdb.filter} for further information about how
   *      to create such a filter expression object.
   *
   * @param f the {@link org.heigit.ohsome.oshdb.filter.FilterExpression} to apply
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapReducerBase<X> filter(FilterExpression f) {
    MapReducerBase<X> ret = this.copy();
    ret.preFilters.add(f::applyOSH);
    ret.filters.add(f::applyOSM);
    // apply geometry filter as first map function
    final List<MapFunction> remainingMappers = List.copyOf(ret.mappers);
    ret.mappers.clear();
    if (this.grouping == Grouping.NONE) {
      // no grouping -> directly filter using the geometries of the snapshot / contribution
      if (isOSMEntitySnapshotViewQuery()) {
        ret = ret.filter(x -> {
          OSMEntitySnapshot s = (OSMEntitySnapshot) x;
          return f.applyOSMEntitySnapshot(s);
        });
      } else if (isOSMContributionViewQuery()) {
        ret = ret.filter(x -> {
          OSMContribution c = (OSMContribution) x;
          return f.applyOSMContribution(c);
        });
      }
    } else if (this.grouping == Grouping.BY_ID) {
      // grouping by entity -> filter each list entry individually
      if (isOSMEntitySnapshotViewQuery()) {
        @SuppressWarnings("unchecked") MapReducerBase<X> filteredListMapper = (MapReducerBase<X>)
            ret.map(x -> (Collection<OSMEntitySnapshot>) x)
                .map(snapshots -> snapshots.stream()
                    .filter(f::applyOSMEntitySnapshot)
                    .collect(Collectors.toCollection(ArrayList::new)))
                .filter(snapshots -> !snapshots.isEmpty());
        ret = filteredListMapper;
      } else if (isOSMContributionViewQuery()) {
        @SuppressWarnings("unchecked") MapReducerBase<X> filteredListMapper = (MapReducerBase<X>)
            ret.map(x -> (Collection<OSMContribution>) x)
                .map(contributions -> contributions.stream()
                    .filter(f::applyOSMContribution)
                    .collect(Collectors.toCollection(ArrayList::new)))
                .filter(contributions -> !contributions.isEmpty());
        ret = filteredListMapper;
      }
    } else {
      throw new UnsupportedOperationException(
          "filtering not implemented in grouping mode " + this.grouping.toString());
    }
    ret.mappers.addAll(remainingMappers);
    return optimizeFilters(ret, f);
  }

  /**
   * Apply a textual filter to this query.
   *
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#syntax">oshdb-filter
   *      readme</a> for a description of the filter syntax.
   *
   * @param f the filter string to apply
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapReducerBase<X> filter(String f) {
    return this.filter(new FilterParser(this.getTagTranslator()).parse(f));
  }

  // -----------------------------------------------------------------------------------------------
  // Grouping and Aggregation
  // Sets how the input data is "grouped", or the output data is "aggregated" into separate chunks.
  // -----------------------------------------------------------------------------------------------

  /**
   * Groups the input data (osm entity snapshot or contributions) by their respective entity's ids
   * before feeding them into further transformation functions. This can be used to do more complex
   * analysis on the osm data, that requires one to know about the full editing history of
   * individual osm entities, e.g., when looking for contributions which got reverted at a later
   * point in time.
   *
   * <p>The values in the returned lists of snapshot or contribution objects are returned in their
   * natural order: i.e. sorted ascending by timestamp.</p>
   *
   * <p>This needs to be called before any `map` or `flatMap` transformation functions have been
   * set. Otherwise a runtime exception will be thrown.</p>
   *
   * @return the MapReducer object which applies its transformations on (by entity id grouped) lists
   *         of the input data
   * @throws UnsupportedOperationException if this is called after some map (or flatMap) functions
   *         have already been set
   * @throws UnsupportedOperationException if this is called when a grouping has already been
   *         activated
   */
  @Override
  @Contract(pure = true)
  public MapReducer<List<X>> groupByEntity() throws UnsupportedOperationException {
    if (this.grouping != Grouping.NONE) {
      throw new UnsupportedOperationException("A grouping is already active on this MapReducer");
    }
    if (!this.mappers.isEmpty()) {
      // for convenience, we allow one to set this function even after some map functions were set.
      // if some map / flatMap functions were already set:
      // "rewind" them first, apply the grouping and then re-apply the map/flatMap functions
      // accordingly
      MapReducerBase<X> ret = this.copy();
      List<MapFunction> mapFunctions = new ArrayList<>(ret.mappers);
      ret.mappers.clear();
      ret.grouping = Grouping.BY_ID;
      @SuppressWarnings("unchecked") // in the reduce step the backend will return a list of items
      MapReducerBase<List<?>> listMapReducer = (MapReducerBase<List<?>>) ret;
      for (MapFunction action : mapFunctions) {
        if (action.isFlatMapper()) {
          listMapReducer = listMapReducer.map((list, root) -> list.stream()
              .flatMap(s -> Streams.stream((Iterable<?>) action.apply(s, root)))
              .collect(Collectors.toList()));
        } else {
          @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
          MapReducerBase<List<?>> mappedResult = listMapReducer.map((list, root) ->
              Lists.transform(list, x -> action.apply(x, root)));
          listMapReducer = mappedResult;
        }
      }
      return listMapReducer.map(List.class::cast);
    } else {
      MapReducerBase<X> ret = this.copy();
      ret.grouping = Grouping.BY_ID;
      @SuppressWarnings("unchecked") // now in the reduce step the backend will return a list of X
      MapReducer<List<X>> result = (MapReducer<List<X>>) ret;
      return result;
    }
  }

  /**
   * Sets a custom aggregation function that is used to group output results into.
   *
   * @param indexer a function that will be called for each input element and returns a value that
   *        will be used to group the results by
   * @param <U> the data type of the values used to aggregate the output. has to be a comparable
   *        type
   * @param zerofill a collection of values that are expected to be present in the result
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   */
  @Override
  @Contract(pure = true)
  public <U extends Comparable<U> & Serializable> MapAggregator<U, X> aggregateBy(
          SerializableFunction<X, U> indexer,
          Collection<U> zerofill
  ) {
    return new MapAggregatorBase<>(this, (data, ignored) -> indexer.apply(data), zerofill);
  }

  /**
   * Sets up automatic aggregation by timestamp.
   *
   * <p>In the OSMEntitySnapshotView, the snapshots' timestamp will be used directly to aggregate
   * results into. In the OSMContributionView, the timestamps of the respective data modifications
   * will be matched to corresponding time intervals (that are defined by the `timestamps` setting
   * here).</p>
   *
   * <p>Cannot be used together with the `groupByEntity()` setting enabled.</p>
   *
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has
   *         been activated
   */
  @Override
  @Contract(pure = true)
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp()
      throws UnsupportedOperationException {
    if (this.grouping != Grouping.NONE) {
      throw new UnsupportedOperationException(
          "automatic aggregateByTimestamp() cannot be used together with the groupByEntity() "
              + "functionality -> try using aggregateByTimestamp(customTimestampIndex) instead");
    }

    // by timestamp indexing function -> for some views we need to match the input data to the list
    SerializableBiFunction<X, Object, OSHDBTimestamp> indexer;
    if (isOSMContributionViewQuery()) {
      final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this.tstamps.get());
      indexer = (ignored, root) -> timestamps.floor(((OSMContribution) root).getTimestamp());
    } else if (isOSMEntitySnapshotViewQuery()) {
      indexer = (ignored, root) -> ((OSMEntitySnapshot) root).getTimestamp();
    } else {
      throw new UnsupportedOperationException(
          "automatic aggregateByTimestamp() only implemented for OSMContribution and "
              + "OSMEntitySnapshot -> try using aggregateByTimestamp(customTimestampIndex) instead"
      );
    }

    return new MapAggregatorBase<>(this, indexer, this.getZerofillTimestamps());
  }

  /**
   * Sets up aggregation by a custom time index.
   *
   * <p>The timestamps returned by the supplied indexing function are matched to the corresponding
   * time intervals.</p>
   *
   * @param indexer a callback function that return a timestamp object for each given data. Note
   *                that if this function returns timestamps outside of the supplied timestamps()
   *                interval results may be undefined
   * @return a MapAggregator object with the equivalent state (settings,
   *         filters, map function, etc.) of the current MapReducer object
   */
  @Override
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp(
          SerializableFunction<X, OSHDBTimestamp> indexer
  ) throws UnsupportedOperationException {
    final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this.tstamps.get());
    final OSHDBTimestamp minTime = timestamps.first();
    final OSHDBTimestamp maxTime = timestamps.last();
    return new MapAggregatorBase<>(this, (data, ignored) -> {
      // match timestamps to the given timestamp list
      OSHDBTimestamp aggregationTimestamp = indexer.apply(data);
      if (aggregationTimestamp == null
          || aggregationTimestamp.compareTo(minTime) < 0
          || aggregationTimestamp.compareTo(maxTime) > 0) {
        throw new OSHDBInvalidTimestampException(
            "Aggregation timestamp outside of time query interval.");
      }
      return timestamps.floor(aggregationTimestamp);
    }, getZerofillTimestamps());
  }

  /**
   * Sets up automatic aggregation by geometries.
   *
   * <p>Cannot be used together with the `groupByEntity()` setting enabled.</p>
   *
   * @param geometries an associated list of polygons and identifiers
   * @param <U> the type of the identifers used to aggregate
   * @param <P> a polygonal geometry type
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has
   *         been activated
   * @throws UnsupportedOperationException when called after any map or flatMap functions are set
   */
  @Override
  @Contract(pure = true)
  public <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal>
      MapAggregator<U, X> aggregateByGeometry(Map<U, P> geometries)
      throws UnsupportedOperationException {
    if (this.grouping != Grouping.NONE) {
      throw new UnsupportedOperationException(
          "aggregateByGeometry() cannot be used together with the groupByEntity() functionality");
    }

    GeometrySplitter<U> gs = new GeometrySplitter<>(geometries);

    MapReducer<? extends Entry<U, ? extends OSHDBMapReducible>> mapRed;
    if (isOSMContributionViewQuery()) {
      mapRed = this.flatMap((ignored, root) ->
          gs.splitOSMContribution((OSMContribution) root).entrySet());
    } else if (isOSMEntitySnapshotViewQuery()) {
      mapRed = this.flatMap((ignored, root) ->
          gs.splitOSMEntitySnapshot((OSMEntitySnapshot) root).entrySet());
    } else {
      throw new UnsupportedOperationException(String.format(
          UNIMPLEMENTED_DATA_VIEW, this.viewClass));
    }
    MapAggregator<U, ?> mapAgg = mapRed
        .aggregateBy(Entry::getKey, geometries.keySet())
        .map(Entry::getValue);
    @SuppressWarnings("unchecked") // no mapper functions have been applied so the type is still X
    MapAggregator<U, X> result = (MapAggregator<U, X>) mapAgg;
    return result;
  }

  // -----------------------------------------------------------------------------------------------
  // Exposed generic reduce.
  // Can be used by experienced users of the api to implement complex queries.
  // These offer full flexibility, but are potentially a bit tricky to work with (see javadoc).
  // -----------------------------------------------------------------------------------------------

  /**
   * Generic map-reduce routine.
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  @Override
  @Contract(pure = true)
  public <S> S reduce(
          SerializableSupplier<S> identitySupplier,
          SerializableBiFunction<S, X, S> accumulator,
          SerializableBinaryOperator<S> combiner) {
    checkTimeout();
    switch (this.grouping) {
      case NONE:
        if (this.mappers.stream().noneMatch(MapFunction::isFlatMapper)) {
          final SerializableFunction<Object, X> mapper = this.getMapper();
          if (isOSMContributionViewQuery()) {
            @SuppressWarnings("Convert2MethodRef")
            // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
            final SerializableFunction<OSMContribution, X> contributionMapper =
                data -> mapper.apply(data);
            return this.mapReduceCellsOSMContribution(
                contributionMapper,
                identitySupplier,
                accumulator,
                combiner
            );
          } else if (isOSMEntitySnapshotViewQuery()) {
            @SuppressWarnings("Convert2MethodRef")
            // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
            final SerializableFunction<OSMEntitySnapshot, X> snapshotMapper =
                data -> mapper.apply(data);
            return this.mapReduceCellsOSMEntitySnapshot(
                snapshotMapper,
                identitySupplier,
                accumulator,
                combiner
            );
          } else {
            throw new UnsupportedOperationException(String.format(
                UNIMPLEMENTED_DATA_VIEW, this.viewClass));
          }
        } else {
          final SerializableFunction<Object, Iterable<X>> flatMapper = this.getFlatMapper();
          if (isOSMContributionViewQuery()) {
            return this.flatMapReduceCellsOSMContributionGroupedById(
                (List<OSMContribution> inputList) -> {
                  List<X> outputList = new LinkedList<>();
                  inputList.stream()
                      .map((SerializableFunction<OSMContribution, Iterable<X>>) flatMapper::apply)
                      .forEach(data -> Iterables.addAll(outputList, data));
                  return outputList;
                }, identitySupplier, accumulator, combiner);
          } else if (isOSMEntitySnapshotViewQuery()) {
            return this.flatMapReduceCellsOSMEntitySnapshotGroupedById(
                (List<OSMEntitySnapshot> inputList) -> {
                  List<X> outputList = new LinkedList<>();
                  inputList.stream()
                      .map((SerializableFunction<OSMEntitySnapshot, Iterable<X>>) flatMapper::apply)
                      .forEach(data -> Iterables.addAll(outputList, data));
                  return outputList;
                }, identitySupplier, accumulator, combiner);
          } else {
            throw new UnsupportedOperationException(String.format(
                UNIMPLEMENTED_DATA_VIEW, this.viewClass));
          }
        }
      case BY_ID:
        final SerializableFunction<Object, Iterable<X>> flatMapper;
        if (this.mappers.stream().noneMatch(MapFunction::isFlatMapper)) {
          final SerializableFunction<Object, X> mapper = this.getMapper();
          flatMapper = data -> Collections.singletonList(mapper.apply(data));
          // todo: check if this is actually necessary, doesn't getFlatMapper() do the "same" in
          // this case? should we add this as optimization case to getFlatMapper()??
        } else {
          flatMapper = this.getFlatMapper();
        }
        if (isOSMContributionViewQuery()) {
          @SuppressWarnings("Convert2MethodRef")
          // having just `flatMapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
          final SerializableFunction<List<OSMContribution>, Iterable<X>> contributionFlatMapper =
              data -> flatMapper.apply(data);
          return this.flatMapReduceCellsOSMContributionGroupedById(
              contributionFlatMapper,
              identitySupplier,
              accumulator,
              combiner
          );
        } else if (isOSMEntitySnapshotViewQuery()) {
          @SuppressWarnings("Convert2MethodRef")
          // having just `flatMapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
          final SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> snapshotFlatMapper =
              data -> flatMapper.apply(data);
          return this.flatMapReduceCellsOSMEntitySnapshotGroupedById(
              snapshotFlatMapper,
              identitySupplier,
              accumulator,
              combiner
          );
        } else {
          throw new UnsupportedOperationException(String.format(
              UNIMPLEMENTED_DATA_VIEW, this.viewClass));
        }
      default:
        throw new UnsupportedOperationException(String.format(
            UNSUPPORTED_GROUPING, this.grouping));
    }
  }

  // -----------------------------------------------------------------------------------------------
  // "Quality of life" helper methods to use the map-reduce functionality more directly and easily
  // for typical queries.
  // Available are: sum, count, average, weightedAverage and uniq.
  // Each one can be used to get results aggregated by timestamp, aggregated by a custom index and
  // not aggregated totals.
  // -----------------------------------------------------------------------------------------------

  // -----------------------------------------------------------------------------------------------
  // "Iterator" like helpers (stream, collect)
  // -----------------------------------------------------------------------------------------------

  /**
   * Returns all results as a Stream.
   *
   * <p>If the used oshdb database backend doesn't implement the stream operation directly, this
   * will fall back to executing `.collect().stream()` instead, which buffers all results in
   * memory first before returning them as a stream.</p>
   *
   * @return a stream with all results returned by the `mapper` function
   */
  @Override
  @Contract(pure = true)
  public Stream<X> stream() {
    try {
      return this.streamInternal();
    } catch (UnsupportedOperationException e) {
      LOG.info("stream not directly supported by chosen backend, falling back to "
          + ".collect().stream()");
      return this.collect().stream();
    }
  }

  @Contract(pure = true)
  private Stream<X> streamInternal() {
    checkTimeout();
    switch (this.grouping) {
      case NONE:
        if (this.mappers.stream().noneMatch(MapFunction::isFlatMapper)) {
          final SerializableFunction<Object, X> mapper = this.getMapper();
          if (isOSMContributionViewQuery()) {
            @SuppressWarnings("Convert2MethodRef")
            // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
            final SerializableFunction<OSMContribution, X> contributionMapper =
                data -> mapper.apply(data);
            return this.mapStreamCellsOSMContribution(contributionMapper);
          } else if (isOSMEntitySnapshotViewQuery()) {
            @SuppressWarnings("Convert2MethodRef")
            // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
            final SerializableFunction<OSMEntitySnapshot, X> snapshotMapper =
                data -> mapper.apply(data);
            return this.mapStreamCellsOSMEntitySnapshot(snapshotMapper);
          } else {
            throw new UnsupportedOperationException(String.format(
                UNIMPLEMENTED_DATA_VIEW, this.viewClass));
          }
        } else {
          final SerializableFunction<Object, Iterable<X>> flatMapper = this.getFlatMapper();
          if (isOSMContributionViewQuery()) {
            return this.flatMapStreamCellsOSMContributionGroupedById(
                (List<OSMContribution> inputList) -> {
                  List<X> outputList = new LinkedList<>();
                  inputList.stream()
                      .map((SerializableFunction<OSMContribution, Iterable<X>>) flatMapper::apply)
                      .forEach(data -> Iterables.addAll(outputList, data));
                  return outputList;
                });
          } else if (isOSMEntitySnapshotViewQuery()) {
            return this.flatMapStreamCellsOSMEntitySnapshotGroupedById(
                (List<OSMEntitySnapshot> inputList) -> {
                  List<X> outputList = new LinkedList<>();
                  inputList.stream()
                      .map((SerializableFunction<OSMEntitySnapshot, Iterable<X>>) flatMapper::apply)
                      .forEach(data -> Iterables.addAll(outputList, data));
                  return outputList;
                });
          } else {
            throw new UnsupportedOperationException(String.format(
                UNIMPLEMENTED_DATA_VIEW, this.viewClass));
          }
        }
      case BY_ID:
        final SerializableFunction<Object, Iterable<X>> flatMapper;
        if (this.mappers.stream().noneMatch(MapFunction::isFlatMapper)) {
          final SerializableFunction<Object, X> mapper = this.getMapper();
          flatMapper = data -> Collections.singletonList(mapper.apply(data));
          // todo: check if this is actually necessary, doesn't getFlatMapper() do the "same" in
          // this case? should we add this as optimization case to getFlatMapper()??
        } else {
          flatMapper = this.getFlatMapper();
        }
        if (isOSMContributionViewQuery()) {
          @SuppressWarnings("Convert2MethodRef")
          // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
          final SerializableFunction<List<OSMContribution>, Iterable<X>> contributionFlatMapper =
              data -> flatMapper.apply(data);
          return this.flatMapStreamCellsOSMContributionGroupedById(contributionFlatMapper);
        } else if (isOSMEntitySnapshotViewQuery()) {
          @SuppressWarnings("Convert2MethodRef")
          // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
          final SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> snapshotFlatMapper =
              data -> flatMapper.apply(data);
          return this.flatMapStreamCellsOSMEntitySnapshotGroupedById(snapshotFlatMapper);
        } else {
          throw new UnsupportedOperationException(String.format(
              UNIMPLEMENTED_DATA_VIEW, this.viewClass));
        }
      default:
        throw new UnsupportedOperationException(String.format(
            UNSUPPORTED_GROUPING, this.grouping));
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Generic map-stream functions (internal).
  // These need to be implemented by the actual db/processing backend!
  // -----------------------------------------------------------------------------------------------

  protected abstract Stream<X> mapStreamCellsOSMContribution(
      SerializableFunction<OSMContribution, X> mapper);

  protected abstract Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper);

  protected abstract Stream<X> mapStreamCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, X> mapper);

  protected abstract Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper);

  // -----------------------------------------------------------------------------------------------
  // Generic map-reduce functions (internal).
  // These need to be implemented by the actual db/processing backend!
  // -----------------------------------------------------------------------------------------------

  /**
   * Generic map-reduce used by the `OSMContributionView`.
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
   *
   * @param mapper a function that's called for each `OSMContribution`
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  protected abstract <R, S> S mapReduceCellsOSMContribution(
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner);

  /**
   * Generic "flat" version of the map-reduce used by the `OSMContributionView`, with by-osm-id
   * grouped input to the `mapper` function.
   *
   * <p>
   * Contrary to the "normal" map-reduce, the "flat" version adds the possibility to return any
   * number of results in the `mapper` function. Additionally, this interface provides the `mapper`
   * function with a list of all `OSMContribution`s of a particular OSM entity. This is used to do
   * more complex analyses that require the full edit history of the respective OSM entities as
   * input.
   * </p>
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
   *
   * @param mapper a function that's called for all `OSMContribution`s of a particular OSM entity;
   *        returns a list of results (which can have any number of entries).
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  protected abstract <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner);

  /**
   * Generic map-reduce used by the `OSMEntitySnapshotView`.
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
   *
   * @param mapper a function that's called for each `OSMEntitySnapshot`
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  protected abstract <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner);

  /**
   * Generic "flat" version of the map-reduce used by the `OSMEntitySnapshotView`, with by-osm-id
   * grouped input to the `mapper` function.
   *
   * <p>
   * Contrary to the "normal" map-reduce, the "flat" version adds the possibility to return any
   * number of results in the `mapper` function. Additionally, this interface provides the `mapper`
   * function with a list of all `OSMContribution`s of a particular OSM entity. This is used to do
   * more complex analyses that require the full list of snapshots of the respective OSM entities as
   * input.
   * </p>
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
   *
   * @param mapper a function that's called for all `OSMEntitySnapshot`s of a particular OSM entity;
   *        returns a list of results (which can have any number of entries)
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  protected abstract <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner);

  // -----------------------------------------------------------------------------------------------
  // Some helper methods for internal use in the mapReduce functions
  // -----------------------------------------------------------------------------------------------

  protected boolean isOSMContributionViewQuery() {
    return OSMContribution.class.isAssignableFrom(this.viewClass);
  }

  protected boolean isOSMEntitySnapshotViewQuery() {
    return OSMEntitySnapshot.class.isAssignableFrom(this.viewClass);
  }

  protected TagInterpreter getTagInterpreter() {
    if (this.tagInterpreter == null) {
      try {
        this.tagInterpreter = new DefaultTagInterpreter(this.getTagTranslator());
      } catch (IOException | ParseException e) {
        throw new OSHDBException(e);
      }
    }
    return this.tagInterpreter;
  }

  protected TagTranslator getTagTranslator() {
    if (this.tagTranslator == null) {
      try {
        if (this.keytables == null) {
          throw new OSHDBKeytablesNotFoundException();
        }
        this.tagTranslator = new TagTranslator(this.keytables.getConnection());
      } catch (OSHDBKeytablesNotFoundException e) {
        throw new OSHDBException(e);
      }
    }
    return this.tagTranslator;
  }

  // Helper that chains multiple oshEntity filters together
  protected OSHEntityFilter getPreFilter() {
    return this.preFilters.isEmpty()
        ? oshEntity -> true
        : oshEntity -> {
          for (SerializablePredicate<OSHEntity> filter : this.preFilters) {
            if (!filter.test(oshEntity)) {
              return false;
            }
          }
          return true;
        };
  }

  // Helper that chains multiple osmEntity filters together
  protected OSMEntityFilter getFilter() {
    return this.filters.isEmpty()
        ? osmEntity -> true
        : osmEntity -> {
          for (SerializablePredicate<OSMEntity> filter : this.filters) {
            if (!filter.test(osmEntity)) {
              return false;
            }
          }
          return true;
        };
  }

  // get all cell ids covered by the current area of interest's bounding box
  protected Iterable<CellIdRange> getCellIdRanges() {
    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
    if (this.bboxFilter == null
        || this.bboxFilter.getMinLongitude() >= this.bboxFilter.getMaxLongitude()
        || this.bboxFilter.getMinLatitude() >= this.bboxFilter.getMaxLatitude()) {
      // return an empty iterable if bbox is not set or empty
      LOG.warn("area of interest not set or empty");
      return Collections.emptyList();
    }
    return grid.bbox2CellIdRanges(this.bboxFilter, true);
  }

  // hack, so that we can use a variable that is of both Geometry and implements Polygonal (i.e.
  // Polygon or MultiPolygon) as required in further processing steps
  @SuppressWarnings("unchecked") // all setters only accept Polygonal geometries
  protected <P extends Geometry & Polygonal> P getPolyFilter() {
    return (P) this.polyFilter;
  }

  // concatenates all applied `map` functions
  private SerializableFunction<Object, X> getMapper() {
    // todo: maybe we can somehow optimize this?? at least for special cases like
    // this.mappers.size() == 1
    return (SerializableFunction<Object, X>) data -> {
      // working with raw Objects since we don't know the actual intermediate types ¯\_(ツ)_/¯
      Object result = data;
      for (MapFunction mapper : this.mappers) {
        if (mapper.isFlatMapper()) {
          assert false : "flatMap callback requested in getMapper";
          throw new UnsupportedOperationException("cannot flat map this");
        } else {
          result = mapper.apply(result, data);
        }
      }
      @SuppressWarnings("unchecked")
      // after applying all mapper functions, the result type is X
      X mappedResult = (X) result;
      return mappedResult;
    };
  }

  // concatenates all applied `flatMap` and `map` functions
  private SerializableFunction<Object, Iterable<X>> getFlatMapper() {
    // todo: maybe we can somehow optimize this?? at least for special cases like
    // this.mappers.size() == 1
    return (SerializableFunction<Object, Iterable<X>>) data -> {
      // working with raw objects since we don't know the actual intermediate types ¯\_(ツ)_/¯
      List<Object> results = new LinkedList<>();
      results.add(data);
      for (MapFunction mapper : this.mappers) {
        List<Object> newResults = new LinkedList<>();
        if (mapper.isFlatMapper()) {
          results.forEach(result ->
              Iterables.addAll(newResults, (Iterable<?>) mapper.apply(result, data)));
        } else {
          results.forEach(result -> newResults.add(mapper.apply(result, data)));
        }
        results = newResults;
      }
      @SuppressWarnings("unchecked")
      // after applying all mapper functions, the result type is List<X>
      Iterable<X> mappedResults = (Iterable<X>) results;
      return mappedResults;
    };
  }

  // gets list of timestamps to use for zerofilling
  Collection<OSHDBTimestamp> getZerofillTimestamps() {
    if (isOSMEntitySnapshotViewQuery()) {
      return this.tstamps.get();
    } else {
      SortedSet<OSHDBTimestamp> result = new TreeSet<>(this.tstamps.get());
      result.remove(result.last());
      return result;
    }
  }

  /**
   * Checks if the current request should be run on a cancelable backend.
   * Produces a log message if not.
   */
  private void checkTimeout() {
    if (this.oshdb.timeoutInMilliseconds().isPresent()) {
      if (!this.isCancelable()) {
        LOG.error("A query timeout was set but the database backend isn't cancelable");
      } else {
        this.timeout = this.oshdb.timeoutInMilliseconds().getAsLong();
      }
    }
  }

  /**
   * Performs optimizations when filtering by a filter expression.
   *
   * <p>It is not always optimal to apply filter expressions directly "out of the box", because
   * it is using the flexible `osmEntityFilter` in the general case. If a filter expression can
   * be rewritten to use the more performant, but less flexible, OSHDB filters (i.e., `osmTag` or
   * `osmType`) this can result in a large performance boost.</p>
   *
   * <p>Currently, the following two optimizations are performed (but more could be feasibly be
   * added in the future:</p>
   *
   * <p><b>basic optimizations:</b> includes simple filter expressions witch can be directly
   * transformed to an (and-chain) of OSHDB filters (like OSM Tags or Types</p>
   *
   * @param mapRed the mapReducer whis the given filter was already applied on.
   * @param filter the filter to optimize.
   * @param <O> the type of the mapReducer to optimize (can be anything).
   * @return a mapReducer with the same semantics as the original one, after some optimizations
   *         were applied.
   */
  private <O> MapReducerBase<O> optimizeFilters(MapReducerBase<O> mapRed, FilterExpression filter) {
    // basic optimizations
    mapRed = optimizeFilters0(mapRed, filter);
    // more advanced optimizations that rely on analyzing the DNF of a filter expression
    try {
      mapRed = optimizeFilters1(mapRed, filter);
    } catch (IllegalStateException ignored) {
      // if a filter cannot be normalized -> just don't perform this optimization step
    }
    return mapRed;
  }

  private <O> MapReducerBase<O> optimizeFilters0(MapReducerBase<O> mapRed,
      FilterExpression filter) {
    // basic optimizations (“low hanging fruit”):
    // single filters, and-combination of single filters, etc.
    if (filter instanceof TagFilterEquals) {
      return mapRed.osmTag(((TagFilterEquals) filter).getTag());
    } else if (filter instanceof TagFilterEqualsAny) {
      OSHDBTagKey key = ((TagFilterEqualsAny) filter).getTag();
      return mapRed.osmTag(key);
    } else if (filter instanceof TypeFilter) {
      return mapRed.osmTypeInternal(EnumSet.of(((TypeFilter) filter).getType()));
    } else if (filter instanceof AndOperator) {
      return optimizeFilters0(optimizeFilters0(mapRed,
          ((AndOperator) filter).getLeftOperand()),
          ((AndOperator) filter).getRightOperand());
    }
    return mapRed;
  }

  private <O> MapReducerBase<O> optimizeFilters1(MapReducerBase<O> mapRed,
      FilterExpression filter) {
    // more advanced optimizations that rely on analyzing the DNF of a filter expression
    List<List<Filter>> filterNormalized = filter.normalize();
    // collect all OSMTypes in all of the clauses
    EnumSet<OSMType> allTypes = EnumSet.noneOf(OSMType.class);
    for (List<Filter> andSubFilter : filterNormalized) {
      EnumSet<OSMType> subTypes = EnumSet.of(OSMType.NODE, OSMType.WAY, OSMType.RELATION);
      for (Filter subFilter : andSubFilter) {
        if (subFilter instanceof TypeFilter) {
          subTypes.retainAll(EnumSet.of(((TypeFilter) subFilter).getType()));
        } else if (subFilter instanceof GeometryTypeFilter) {
          subTypes.retainAll(((GeometryTypeFilter) subFilter).getOSMTypes());
        }
      }
      allTypes.addAll(subTypes);
    }
    mapRed = mapRed.osmTypeInternal(allTypes);
    // (todo) intelligently group queried tags
    /*
     * here, we could optimize a few situations further: when a specific tag or key is used in all
     * branches of the filter: run mapRed.osmTag the set of tags which are present in any branches:
     * run mapRed.osmTag(list) (note that for this all branches need to have at least one
     * TagFilterEquals or TagFilterEqualsAny) related: https://github.com/GIScience/oshdb/pull/210
     */
    return mapRed;
  }

  private String currentDate() {
    var formatter = new SimpleDateFormat("yyyy-MM-dd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    return formatter.format(new Date());
  }
}
