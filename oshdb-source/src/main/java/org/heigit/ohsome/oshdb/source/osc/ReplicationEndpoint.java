package org.heigit.ohsome.oshdb.source.osc;

import static java.lang.String.format;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofDays;
import static java.time.Duration.ofHours;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.util.Optional.ofNullable;
import static org.heigit.ohsome.oshdb.source.osc.ReplicationState.getServerState;
import static org.heigit.ohsome.oshdb.source.osc.ReplicationState.getState;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import org.heigit.ohsome.oshdb.source.ReplicationInfo;

public class ReplicationEndpoint {

  private static final String OSM_REPLICATION_MINUTE = "https://planet.osm.org/replication/minute/";
  private static final String OSM_REPLICATION_HOUR = "https://planet.osm.org/replication/hour/";
  private static final String OSM_REPLICATION_DAY = "https://planet.osm.org/replication/day/";

  public static final ReplicationEndpoint OSM_ORG_MINUTELY;
  public static final ReplicationEndpoint OSM_ORG_HOURLY;
  public static final ReplicationEndpoint OSM_ORG_DAILY;

  private static final Map<String, ReplicationEndpoint> OSM_ORG_REPLICATION_ENDPOINTS;

  static {
      OSM_ORG_MINUTELY = new ReplicationEndpoint(OSM_REPLICATION_MINUTE, ofMinutes(1), ZERO);
      OSM_ORG_HOURLY = new ReplicationEndpoint(OSM_REPLICATION_HOUR, ofHours(1), ofMillis(2));
      OSM_ORG_DAILY = new ReplicationEndpoint(OSM_REPLICATION_DAY, ofDays(1), ofMinutes(20));
      OSM_ORG_REPLICATION_ENDPOINTS = Map.of(
          OSM_ORG_MINUTELY.url(), OSM_ORG_MINUTELY,
          OSM_ORG_HOURLY.url(), OSM_ORG_HOURLY,
          OSM_ORG_DAILY.url(), OSM_ORG_DAILY);
  }

  private final String url;
  private final Duration frequency;
  private final Duration delay;

  public ReplicationEndpoint(String url, Duration frequency, Duration delay) {
    this.url = url;
    this.frequency = frequency;
    this.delay = delay;
  }

  public ReplicationState serverState() throws IOException {
    return getServerState(this);
  }

  public ReplicationState state(int sequenceNumber) throws IOException {
    return getState(this, sequenceNumber);
  }

  public String url() {
    return url;
  }

  public ZonedDateTime nextTimestamp(ReplicationState state) {
    return state.getTimestamp().plus(frequency).plus(delay);
  }

  @Override
  public String toString() {
    return format("ReplicationEndpoint [url=%s, frequency=%s, delay=%s]", url, frequency, delay);
  }

  public static ReplicationState stateFromInfo(ReplicationInfo info) {
    if (info instanceof ReplicationState state) {
      return state;
    }
    var endpoint = ofNullable(OSM_ORG_REPLICATION_ENDPOINTS.get(info.getBaseUrl())).orElseThrow();
    return new ReplicationState(endpoint, info.getTimestamp(), info.getSequenceNumber());
  }
}
