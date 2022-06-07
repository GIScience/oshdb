package org.heigit.ohsome.oshdb.api.mapreducer;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.heigit.ohsome.oshdb.util.geometry.Geo.clip;
import static org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder.boundingBoxOf;
import static org.heigit.ohsome.oshdb.util.time.IsoDateTimeParser.parseIsoDateTime;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.TreeSet;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.ohsome.oshdb.util.taginterpreter.DefaultTagInterpreter;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestampList;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

/**
 * OSHDBView builder class.
 *
 * @param <T> OSHDBMapReducible type
 */
public abstract class OSHDBView<T> {

  /**
   * Types for OSHDBViews.
   *
   */
  public enum ViewType {
    /**
     * Snapshot view type.
     */
    SNAPSHOT,
    /**
     * Contribution view type.
     */
    CONTRIBUTION;
  }

  //settings and filters
  protected OSHDBTimestampList tstamps = new OSHDBTimestamps(
      "2008-01-01",
      currentDate(),
      OSHDBTimestamps.Interval.MONTHLY);
  protected OSHDBBoundingBox bboxFilter = bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0);
  protected Geometry polyFilter = null;

  protected final List<String> filters = new ArrayList<>();
  protected final List<FilterExpression> filterExpressions = new ArrayList<>();

  protected TagTranslator tagTranslator = null;
  private OSHDBJdbc keytables;
  private TagInterpreter tagInterpreter;

  /**
   * Get type for this view.
   *
   * @return view type
   */
  public abstract ViewType type();

  public abstract MapReducer<T> on(OSHDBDatabase oshdb) throws OSHDBException;

  public final OSHDBTimestampList getTimestamps() {
    return tstamps;
  }

  public final OSHDBBoundingBox getBboxFilter() {
    return bboxFilter;
  }

  public final Geometry getPolyFilter() {
    return polyFilter;
  }

  public final List<String> getFilters() {
    return filters;
  }

  public final List<FilterExpression> getFilterExpressions() {
    return filterExpressions;
  }

  public final TagTranslator getTagTranslator(OSHDBDatabase oshdb) {
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

  /**
   * Sets the keytables database to use in the calculations to resolve strings (osm tags, roles)
   * into internally used identifiers. If this function is never called, the main database
   * (specified during the construction of this object) is used for this.
   *
   * @param keytables the database to use for resolving strings into internal identifiers
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public OSHDBView<T> keytables(OSHDBJdbc keytables) {
    this.keytables = keytables;
    return this;
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
  public OSHDBView<T> tagInterpreter(TagInterpreter tagInterpreter) {
    this.tagInterpreter = tagInterpreter;
    return this;
  }

  public TagInterpreter getTagInterpreter(OSHDBDatabase oshdb) throws IOException, ParseException {
    if (this.tagInterpreter == null) {
      return new DefaultTagInterpreter(this.getTagTranslator(oshdb));
    }
    return tagInterpreter;
  }

  /**
   * Set the area of interest to the given bounding box. Only objects inside or clipped by this bbox
   * will be passed on to the analysis' `mapper` function.
   *
   * @param bboxFilter the bounding box to query the data in
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public OSHDBView<T> areaOfInterest(@NotNull OSHDBBoundingBox bboxFilter) {
    if (this.polyFilter == null) {
      this.bboxFilter = bboxFilter.intersection(bboxFilter);
    } else {
      this.polyFilter = clip(this.polyFilter, bboxFilter);
      this.bboxFilter = boundingBoxOf(this.polyFilter.getEnvelopeInternal());
    }
    return this;
  }

  /**
   * Set the area of interest to the given polygon. Only objects inside or clipped by this polygon
   * will be passed on to the analysis' `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public <P extends Geometry & Polygonal> OSHDBView<T> areaOfInterest(P polygonFilter) {
    if (this.polyFilter == null) {
      this.polyFilter = clip(polygonFilter, this.bboxFilter);
    } else {
      this.polyFilter = clip(polygonFilter, this.polyFilter);
    }
    this.bboxFilter = boundingBoxOf(this.polyFilter.getEnvelopeInternal());
    return this;
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

  public OSHDBView<T> timestamps(OSHDBTimestampList tstamps) {
    this.tstamps = tstamps;
    return this;
  }

  public OSHDBView<T> timestamps(String isoDateStart, String isoDateEnd,
      OSHDBTimestamps.Interval interval) {
    return this.timestamps(new OSHDBTimestamps(isoDateStart, isoDateEnd, interval));
  }

  /**
   * Sets a single timestamp for which to perform the analysis at.
   *
   * <p>Useful in combination with the OSMEntitySnapshotView when not performing further aggregation
   * by timestamp.</p>
   *
   * <p>See {@link #timestamps(OSHDBTimestampList)} for further information.</p>
   *
   * @param isoDate an ISO 8601 date string representing the date of the analysis
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public OSHDBView<T> timestamps(String isoDate) {
    return this.timestamps(isoDate, isoDate);
  }

  /**
   * Sets multiple arbitrary timestamps for which to perform the analysis.
   *
   * <p>Note for programmers wanting to use this method to supply an arbitrary number (n&gt;=1) of
   * timestamps: You may supply the same time string multiple times, which will be de-duplicated
   * internally. E.g. you can call the method like this:
   * <code>.timestamps(dateArr[0], dateArr[0], dateArr)</code>
   * </p>
   *
   * <p>See {@link #timestamps(OSHDBTimestampList)} for further information.</p>
   *
   * @param isoDateFirst an ISO 8601 date string representing the start date of the analysis
   * @param isoDateSecond an ISO 8601 date string representing the second date of the analysis
   * @param isoDateMore more ISO 8601 date strings representing the remaining timestamps of the
   *        analysis
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public OSHDBView<T> timestamps(String isoDateFirst, String isoDateSecond,
      String... isoDateMore) {
    var timestamps = new TreeSet<OSHDBTimestamp>();
    timestamps.add(new OSHDBTimestamp(parseIsoDateTime(isoDateFirst).toEpochSecond()));
    timestamps.add(new OSHDBTimestamp(parseIsoDateTime(isoDateSecond).toEpochSecond()));
    for (String isoDate : isoDateMore) {
      timestamps.add(new OSHDBTimestamp(parseIsoDateTime(isoDate).toEpochSecond()));
    }
    return this.timestamps(() -> timestamps);
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
  public OSHDBView<T> filter(FilterExpression f) {
    filterExpressions.add(f);
    return this;
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
  public OSHDBView<T> filter(String f) {
    filters.add(f);
    return this;
  }

  private String currentDate() {
    var formatter = new SimpleDateFormat("yyyy-MM-dd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    return formatter.format(new Date());
  }
}
