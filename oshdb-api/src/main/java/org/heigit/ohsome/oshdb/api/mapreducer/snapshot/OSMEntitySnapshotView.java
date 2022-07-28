package org.heigit.ohsome.oshdb.api.mapreducer.snapshot;

import static org.heigit.ohsome.oshdb.util.time.IsoDateTimeParser.parseIsoDateTime;
import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshotImpl;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestampList;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Polygon;

public class OSMEntitySnapshotView extends OSHDBView<OSMEntitySnapshotView>{

  /**
   * Set the timestamps for which to perform the analysis.
   * <p>
   * Depending on the *View*, this has slightly different semantics:
   * </p>
   * <ul>
   * <li>For the OSMEntitySnapshotView it will set the time slices at which to take the "snapshots"
   * </li>
   * <li>For the OSMContributionView it will set the time interval in which to look for osm
   * contributions (only the first and last timestamp of this list are contributing).</li>
   * </ul>
   * Additionally, these timestamps are used in the `aggregateByTimestamp` functionality.
   *
   * @param tstamps an object (implementing the OSHDBTimestampList interface) which provides the
   *        timestamps to do the analysis for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */

  public OSMEntitySnapshotView timestamps(OSHDBTimestampList tstamps) {
    this.tstamps = tstamps;
    return this;
  }

  public OSMEntitySnapshotView timestamps(String isoDateStart, String isoDateEnd,
      OSHDBTimestamps.Interval interval) {
    return this.timestamps(new OSHDBTimestamps(isoDateStart, isoDateEnd, interval));
  }

  /**
   * Sets a single timestamp for which to perform the analysis at.
   * <p>
   * Useful in combination with the OSMEntitySnapshotView when not performing further aggregation by
   * timestamp.
   * </p>
   * <p>
   * See {@link #timestamps(OSHDBTimestampList)} for further information.
   * </p>
   *
   * @param isoDate an ISO 8601 date string representing the date of the analysis
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public OSMEntitySnapshotView timestamps(String isoDate) {
    return this.timestamps(isoDate, isoDate);
  }

  /**
   * Sets multiple arbitrary timestamps for which to perform the analysis.
   * <p>
   * Note for programmers wanting to use this method to supply an arbitrary number (n&gt;=1) of
   * timestamps: You may supply the same time string multiple times, which will be de-duplicated
   * internally. E.g. you can call the method like this:
   * <code>.timestamps(dateArr[0], dateArr[0], dateArr)</code>
   * </p>
   * <p>
   * See {@link #timestamps(OSHDBTimestampList)} for further information.
   * </p>
   *
   * @param isoDateFirst an ISO 8601 date string representing the start date of the analysis
   * @param isoDateSecond an ISO 8601 date string representing the second date of the analysis
   * @param isoDateMore more ISO 8601 date strings representing the remaining timestamps of the
   *        analysis
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  public OSMEntitySnapshotView timestamps(String isoDateFirst, String isoDateSecond, String... isoDateMore) {
    var timestamps = new TreeSet<OSHDBTimestamp>();
    timestamps.add(new OSHDBTimestamp(parseIsoDateTime(isoDateFirst).toEpochSecond()));
    timestamps.add(new OSHDBTimestamp(parseIsoDateTime(isoDateSecond).toEpochSecond()));
    for (String isoDate : isoDateMore) {
      timestamps.add(new OSHDBTimestamp(parseIsoDateTime(isoDate).toEpochSecond()));
    }
    return this.timestamps(() -> timestamps);
  }

  public static OSMEntitySnapshotView view() {
    return new OSMEntitySnapshotView();
  }

  @Override
  protected OSMEntitySnapshotView instance() {
    return this;
  }

  public MapReducerSnapshot on(OSHDBDatabase oshdb) throws IOException, ParseException {
    var tagInterpretor = getTagInterpreter();
    if (tagInterpretor == null) {
      tagInterpretor = oshdb.getTagInterpreter();
    }
    parseFilters(oshdb.getTagTranslator());
    var cellIterator = new CellIterator(tstamps.get(), bboxFilter, (Polygon) polyFilter,
        tagInterpretor, getPreFilter(), getFilter(), false);
    return new MapReducerSnapshot(this, oshdb,
        osh -> cellIterator.iterateByTimestamps(List.of(osh), false)
        .map(OSMEntitySnapshotImpl::new));
  }
}
