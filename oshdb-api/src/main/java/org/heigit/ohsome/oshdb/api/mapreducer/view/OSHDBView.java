package org.heigit.ohsome.oshdb.api.mapreducer.view;

import static java.util.Collections.emptyList;
import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.heigit.ohsome.oshdb.util.geometry.Geo.clip;
import static org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder.boundingBoxOf;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import org.heigit.ohsome.oshdb.OSHDB;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
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
import org.heigit.ohsome.oshdb.util.function.OSHEntityFilter;
import org.heigit.ohsome.oshdb.util.function.OSMEntityFilter;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.geometry.fip.FastBboxOutsidePolygon;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestampList;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public abstract class OSHDBView<V extends OSHDBView> {

  protected OSHDBBoundingBox bboxFilter = bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0);
  protected Geometry polyFilter = null;
  protected FastBboxOutsidePolygon bboxOutsidePolygon;
  protected OSHDBTimestampList tstamps =
      new OSHDBTimestamps("2008-01-01", currentDate(), OSHDBTimestamps.Interval.MONTHLY);




  protected EnumSet<OSMType> typeFilter = EnumSet.of(OSMType.NODE, OSMType.WAY, OSMType.RELATION);
  protected final List<String> filtersStrings = new ArrayList<>();
  protected final List<SerializablePredicate<OSHEntity>> preFilters = new ArrayList<>();
  protected final List<SerializablePredicate<OSMEntity>> filters = new ArrayList<>();

  //  protected TagTranslator tagTranslator = null;
  protected TagInterpreter tagInterpreter;

  protected abstract V instance();

  public final OSHDBBoundingBox getBboxFilter() {
    return bboxFilter;
  }

  @SuppressWarnings("unchecked") // all setters only accept Polygonal geometries
  public <P extends Geometry & Polygonal> P getPolyFilter() {
    return (P) this.polyFilter;
  }

  public final OSHDBTimestampList getTimestamps() {
    return tstamps;
  }

  public final List<SerializablePredicate<OSMEntity>> getFilters() {
    return filters;
  }

  public OSMEntityFilter getFilter() {
    return filters.isEmpty() ? x -> true : osm -> filters.stream().allMatch(f -> f.test(osm));
  }

  //  public final TagTranslator getTagTranslator() {
  //    if (this.tagTranslator == null) {
  //      try {
  //        if (this.keytables == null) {
  //          throw new OSHDBKeytablesNotFoundException();
  //        }
  //        this.tagTranslator = new TagTranslator(this.keytables.getConnection());
  //      } catch (OSHDBKeytablesNotFoundException e) {
  //        throw new OSHDBException(e);
  //      }
  //    }
  //    return this.tagTranslator;
  //  }

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
  public V tagInterpreter(TagInterpreter tagInterpreter) {
    this.tagInterpreter = tagInterpreter;
    return instance();
  }

  /**
   * Set the area of interest to the given bounding box. Only objects inside or clipped by this bbox
   * will be passed on to the analysis' `mapper` function.
   *
   * @param bboxFilter the bounding box to query the data in
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public V areaOfInterest(@NotNull OSHDBBoundingBox bboxFilter) {
    if (this.polyFilter == null) {
      this.bboxFilter = bboxFilter.intersection(bboxFilter);
    } else {
      this.polyFilter = clip(this.polyFilter, bboxFilter);
      this.bboxFilter = boundingBoxOf(this.polyFilter.getEnvelopeInternal());
      this.bboxOutsidePolygon = new FastBboxOutsidePolygon(getPolyFilter());
    }
    return instance();
  }

  /**
   * Set the area of interest to the given polygon. Only objects inside or clipped by this polygon
   * will be passed on to the analysis' `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public <P extends Geometry & Polygonal> V areaOfInterest(P polygonFilter) {
    if (this.polyFilter == null) {
      this.polyFilter = clip(polygonFilter, this.bboxFilter);
    } else {
      this.polyFilter = clip(polygonFilter, this.polyFilter);
    }
    this.bboxFilter = boundingBoxOf(this.polyFilter.getEnvelopeInternal());
    this.bboxOutsidePolygon = new FastBboxOutsidePolygon(getPolyFilter());
    return instance();
  }

  protected void parseFilters(TagTranslator tagTranslator) {
    var parser = new FilterParser(tagTranslator);
    for (var filter : filtersStrings) {
      var expression = parser.parse(filter);
      filter(expression);
    }
  }

  /**
   * Apply a custom filter expression to this query.
   *
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#readme">oshdb-filter
   *      readme</a> and {@link org.heigit.ohsome.oshdb.filter} for further information about how to
   *      create such a filter expression object.
   * @param f the {@link org.heigit.ohsome.oshdb.filter.FilterExpression} to apply
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public V filter(FilterExpression f) {
    preFilters.add(f::applyOSH);
    filters.add(f::applyOSM);
    optimizeFilters(f);
    return instance();
  }

  /**
   * Apply a textual filter to this query.
   *
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#syntax">oshdb-filter
   *      readme</a> for a description of the filter syntax.
   * @param f the filter string to apply
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public V filter(String f) {
    filtersStrings.add(f);
    return instance();
  }

  public TagInterpreter getTagInterpreter() throws IOException, ParseException {
    return tagInterpreter;
  }

  protected String currentDate() {
    var formatter = new SimpleDateFormat("yyyy-MM-dd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    return formatter.format(new Date());
  }

  /**
   * Performs optimizations when filtering by a filter expression.
   *
   * <p>
   * It is not always optimal to apply filter expressions directly "out of the box", because it is
   * using the flexible `osmEntityFilter` in the general case. If a filter expression can be
   * rewritten to use the more performant, but less flexible, OSHDB filters (i.e., `osmTag` or
   * `osmType`) this can result in a large performance boost.
   * </p>
   *
   * <p>
   * Currently, the following two optimizations are performed (but more could be feasibly be added
   * in the future:
   * </p>
   *
   * <p>
   * <b>basic optimizations:</b> includes simple filter expressions witch can be directly
   * transformed to an (and-chain) of OSHDB filters (like OSM Tags or Types
   * </p>
   *
   * @param mapRed the mapReducer whis the given filter was already applied on.
   * @param filter the filter to optimize.
   * @param <O> the type of the mapReducer to optimize (can be anything).
   * @return a mapReducer with the same semantics as the original one, after some optimizations were
   *         applied.
   */
  private void optimizeFilters(FilterExpression filter) {
    // basic optimizations
    optimizeFilters0(filter);
    // more advanced optimizations that rely on analyzing the DNF of a filter expression
    try {
      optimizeFilters1(filter);
    } catch (IllegalStateException ignored) {
      // if a filter cannot be normalized -> just don't perform this optimization step
    }
  }

  private void optimizeFilters0(FilterExpression filter) {
    // basic optimizations (“low hanging fruit”):
    // single filters, and-combination of single filters, etc.
    if (filter instanceof TagFilterEquals) {
      var tag = ((TagFilterEquals) filter).getTag();
      preFilters.add(oshEntity -> oshEntity.hasTagKey(tag.getKey()));
      filters.add(osmEntity -> osmEntity.getTags().hasTagValue(tag.getKey(), tag.getValue()));
    } else if (filter instanceof TagFilterEqualsAny) {
      var tagKey = ((TagFilterEqualsAny) filter).getTag();
      preFilters.add(oshEntity -> oshEntity.hasTagKey(tagKey));
      filters.add(osmEntity -> osmEntity.getTags().hasTagKey(tagKey));
    } else if (filter instanceof TypeFilter) {
      Set<OSMType> types = EnumSet.of(((TypeFilter) filter).getType());
      // return mapRed.osmTypeInternal(EnumSet.of(((TypeFilter) filter).getType()));
      types = Sets.intersection(typeFilter, types);
      if (types.isEmpty()) {
        typeFilter = EnumSet.noneOf(OSMType.class);
      } else {
        typeFilter = EnumSet.copyOf(types);
      }
    } else if (filter instanceof AndOperator) {
      optimizeFilters0(((AndOperator) filter).getLeftOperand());
      optimizeFilters0(((AndOperator) filter).getRightOperand());
    }
  }

  private void optimizeFilters1(FilterExpression filter) {
    // more advanced optimizations that rely on analyzing the DNF of a filter expression
    List<List<Filter>> filterNormalized = filter.normalize();
    // collect all OSMTypes in all of the clauses
    Set<OSMType> allTypes = EnumSet.noneOf(OSMType.class);
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
    allTypes = Sets.intersection(typeFilter, allTypes);
    if (allTypes.isEmpty()) {
      typeFilter = EnumSet.noneOf(OSMType.class);
    } else {
      typeFilter = EnumSet.copyOf(allTypes);
    }
    // (todo) intelligently group queried tags
    /*
     * here, we could optimize a few situations further: when a specific tag or key is used in all
     * branches of the filter: run mapRed.osmTag the set of tags which are present in any branches:
     * run mapRed.osmTag(list) (note that for this all branches need to have at least one
     * TagFilterEquals or TagFilterEqualsAny) related: https://github.com/GIScience/oshdb/pull/210
     */
  }

  public OSHEntityFilter getPreFilter() {
    return preFilters.isEmpty() ? x -> true : osh -> preFilters.stream().allMatch(f -> f.test(osh));
  }

  protected boolean preFilter(OSHEntity osh) {
    return osh.getBoundable().intersects(bboxFilter)
        && (preFilters.isEmpty() || preFilters.stream().allMatch(f -> f.test(osh)))
        && (polyFilter == null || bboxOutsidePolygon.test(osh.getBoundable()));
  }

  public EnumSet<OSMType> getTypeFilter() {
    return typeFilter;
  }

  // get all cell ids covered by the current area of interest's bounding box
  public Iterable<CellIdRange> getCellIdRanges() {
    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
    if (this.bboxFilter == null
        || this.bboxFilter.getMinLongitude() >= this.bboxFilter.getMaxLongitude()
        || this.bboxFilter.getMinLatitude() >= this.bboxFilter.getMaxLatitude()) {
      // return an empty iterable if bbox is not set or empty
      return emptyList();
    }
    return grid.bbox2CellIdRanges(this.bboxFilter, true);
  }
}
