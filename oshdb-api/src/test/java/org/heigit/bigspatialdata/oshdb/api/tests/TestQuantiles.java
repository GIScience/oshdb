/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.function.DoubleUnaryOperator;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;

/**
 *
 */
public class TestQuantiles {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8.651133,49.387611,8.6561,49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2015-01-01");
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2015-01-01");

  private final double REQUIRED_ACCURACY = 1E-4;

  public TestQuantiles() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private void assertApproximateQuantiles(
      List<? extends Number> values, double quantile, double result) {

    double quantileIndex = (values.size() - 1) * quantile;
    int quantileBoundLower = (int) Math.floor(quantileIndex);
    double quantileAmountUpper = quantileIndex - quantileBoundLower;
    int quantileBoundUpper = (int) Math.ceil(quantileIndex);
    double quantileAmountLower = 1 - quantileAmountUpper;
    double expectedResult = (
        quantileAmountLower * values.get(quantileBoundLower).doubleValue() +
        quantileAmountUpper * values.get(quantileBoundUpper).doubleValue()
    );

    assertEquals(expectedResult, result, expectedResult * REQUIRED_ACCURACY);
  }

  // MapReducer

  private MapReducer<OSMEntitySnapshot> createMapReducer() {
    return OSMEntitySnapshotView.on(oshdb)
        .timestamps(timestamps1)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .areaOfInterest(bbox);
  }

  @Test
  public void testMedian() throws Exception {
    MapReducer<Integer> mr = this.createMapReducer()
        .map(s -> s.getGeometry().getCoordinates().length);
    List<Integer> fullResult = mr.collect();
    Collections.sort(fullResult);

    assertApproximateQuantiles(fullResult, 0.5, mr.estimatedMedian());
  }

  @Test
  public void testQuantile() throws Exception {
    MapReducer<Integer> mr = this.createMapReducer()
        .map(s -> s.getGeometry().getCoordinates().length);
    List<Integer> fullResult = mr.collect();
    Collections.sort(fullResult);

    assertApproximateQuantiles(fullResult, 0.8, mr.estimatedQuantile(0.8));
  }

  @Test
  public void testQuantiles() throws Exception {
    MapReducer<Integer> mr = this.createMapReducer()
        .map(s -> s.getGeometry().getCoordinates().length);
    List<Integer> fullResult = mr.collect();
    Collections.sort(fullResult);

    List<Double> qs = Arrays.asList(0.0, 0.2, 0.4, 0.6, 0.8, 1.0);
    List<Double> quantiles = mr.estimatedQuantiles(qs);

    for (Double quantile : quantiles) {
      assertApproximateQuantiles(fullResult, qs.get(quantiles.indexOf(quantile)), quantile);
    }
  }

  @Test
  public void testQuantilesFunction() throws Exception {
    MapReducer<Integer> mr = this.createMapReducer()
        .map(s -> s.getGeometry().getCoordinates().length);
    List<Integer> fullResult = mr.collect();
    Collections.sort(fullResult);

    List<Double> qs = Arrays.asList(0.0, 0.2, 0.4, 0.6, 0.8, 1.0);
    DoubleUnaryOperator quantilesFunction = mr.estimatedQuantiles();

    for (Double q : qs) {
      assertApproximateQuantiles(fullResult, q, quantilesFunction.applyAsDouble(q));
    }
  }

  // MapAggregator

  private MapAggregator<OSHDBTimestamp, OSMEntitySnapshot> createMapAggregator() {
    return OSMEntitySnapshotView.on(oshdb)
        .timestamps(timestamps2)
        .osmType(OSMType.WAY)
        .osmTag("building", "yes")
        .areaOfInterest(bbox)
        .aggregateByTimestamp();
  }

  @Test
  public void testMedianMapAggregator() throws Exception {
    MapAggregator<OSHDBTimestamp, Integer> mr = this.createMapAggregator()
        .map(s -> s.getGeometry().getCoordinates().length);
    SortedMap<OSHDBTimestamp, List<Integer>> fullResult = mr.collect();
    fullResult.values().forEach(Collections::sort);

    SortedMap<OSHDBTimestamp, Double> medians = mr.estimatedQuantile(0.8);

    medians.forEach((ts, median) ->
        assertApproximateQuantiles(fullResult.get(ts), 0.8, median)
    );
  }

  @Test
  public void testQuantileMapAggregator() throws Exception {
    MapAggregator<OSHDBTimestamp, Integer> mr = this.createMapAggregator()
        .map(s -> s.getGeometry().getCoordinates().length);
    SortedMap<OSHDBTimestamp, List<Integer>> fullResult = mr.collect();
    fullResult.values().forEach(Collections::sort);

    SortedMap<OSHDBTimestamp, Double> quantiles = mr.estimatedQuantile(0.8);

    quantiles.forEach((ts, quantile) ->
        assertApproximateQuantiles(fullResult.get(ts), 0.8, quantile)
    );
  }

  @Test
  public void testQuantilesMapAggregator() throws Exception {
    MapAggregator<OSHDBTimestamp, Integer> mr = this.createMapAggregator()
        .map(s -> s.getGeometry().getCoordinates().length);
    SortedMap<OSHDBTimestamp, List<Integer>> fullResult = mr.collect();
    fullResult.values().forEach(Collections::sort);

    List<Double> qs = Arrays.asList(0.0, 0.2, 0.4, 0.6, 0.8, 1.0);
    SortedMap<OSHDBTimestamp, List<Double>> quantiless = mr.estimatedQuantiles(qs);

    quantiless.forEach((ts, quantiles) -> {
      for (Double quantile : quantiles) {
        assertApproximateQuantiles(
            fullResult.get(ts),
            qs.get(quantiles.indexOf(quantile)),
            quantile
        );
      }
    });
  }

  @Test
  public void testQuantilesFunctionMapAggregator() throws Exception {
    MapAggregator<OSHDBTimestamp, Integer> mr = this.createMapAggregator()
        .map(s -> s.getGeometry().getCoordinates().length);
    SortedMap<OSHDBTimestamp, List<Integer>> fullResult = mr.collect();
    fullResult.values().forEach(Collections::sort);

    List<Double> qs = Arrays.asList(0.0, 0.2, 0.4, 0.6, 0.8, 1.0);
    SortedMap<OSHDBTimestamp, DoubleUnaryOperator> quantilesFunctions = mr.estimatedQuantiles();

    quantilesFunctions.forEach((ts, quantilesFunction) -> {
      for (Double q : qs) {
        assertApproximateQuantiles(
            fullResult.get(ts),
            q,
            quantilesFunction.applyAsDouble(q)
        );
      }
    });
  }

}
