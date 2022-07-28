package org.heigit.ohsome.oshdb.api.mapreducer.contribution;

import static org.heigit.ohsome.oshdb.util.time.IsoDateTimeParser.parseIsoDateTime;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.api.object.OSMContributionImpl;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestampList;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Polygon;

public class OSMContributionView extends OSHDBView<OSMContributionView> {

  /**
   * Set the timestamps for which to perform the analysis.
   * <p>For the OSMContributionView it will set the time interval in which to look for osm
   * contributions (only the first and last timestamp of this list are contributing).</p>

   * Additionally, these timestamps are used in the `aggregateByTimestamp` functionality.
   *
   * @param tstamps an object (implementing the OSHDBTimestampList interface) which provides the
   *        timestamps to do the analysis for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */

  public OSMContributionView timestamps(OSHDBTimestampList tstamps) {
    this.tstamps = tstamps;
    return this;
  }

  public OSMContributionView timestamps(String isoDateStart, String isoDateEnd,
      OSHDBTimestamps.Interval interval) {
    return this.timestamps(new OSHDBTimestamps(isoDateStart, isoDateEnd, interval));
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
  public OSMContributionView timestamps(String isoDateFirst, String isoDateSecond, String... isoDateMore) {
    var timestamps = new TreeSet<OSHDBTimestamp>();
    timestamps.add(new OSHDBTimestamp(parseIsoDateTime(isoDateFirst).toEpochSecond()));
    timestamps.add(new OSHDBTimestamp(parseIsoDateTime(isoDateSecond).toEpochSecond()));
    for (String isoDate : isoDateMore) {
      timestamps.add(new OSHDBTimestamp(parseIsoDateTime(isoDate).toEpochSecond()));
    }
    return this.timestamps(() -> timestamps);
  }

  public static OSMContributionView view() {
    return new OSMContributionView();
  }

  @Override
  protected OSMContributionView instance() {
    return this;
  }

  public MapReducerContribution on(OSHDBDatabase oshdb) throws IOException, ParseException {
    var tagInterpretor = getTagInterpreter();
    if (tagInterpretor == null) {
      tagInterpretor = oshdb.getTagInterpreter();
    }
    parseFilters(oshdb.getTagTranslator());
    var cellIterator = new CellIterator(tstamps.get(), bboxFilter, (Polygon) polyFilter,
        tagInterpretor, getPreFilter(), getFilter(), false);
    return new MapReducerContribution(this, oshdb,
        osh -> cellIterator.iterateByContribution(List.of(osh), false)
        .map(OSMContributionImpl::new));
  }

}
