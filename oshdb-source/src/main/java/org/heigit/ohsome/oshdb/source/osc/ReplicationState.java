package org.heigit.ohsome.oshdb.source.osc;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.net.URI.create;
import static java.net.http.HttpResponse.BodyHandlers.ofInputStream;
import static java.time.Duration.ofSeconds;
import static reactor.core.publisher.Flux.using;

import com.google.common.io.Closeables;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.source.OSMSource;
import org.heigit.ohsome.oshdb.source.ReplicationInfo;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

public class ReplicationState implements ReplicationInfo, OSMSource {
  protected static final Logger Log = LoggerFactory.getLogger(ReplicationState.class);

  private static final HttpClient httpClient = HttpClient.newBuilder()
      .version(Version.HTTP_2)
      .connectTimeout(ofSeconds(10))
      .build();

  private static final DecimalFormat sequenceformatter;

  static {
    var formatSymbols = new DecimalFormatSymbols(Locale.US);
    formatSymbols.setGroupingSeparator('/');
    sequenceformatter = new DecimalFormat("000,000,000", formatSymbols);
  }

  public static ReplicationState getServerState(ReplicationEndpoint endpoint)
      throws IOException, InterruptedException {
    return getState(endpoint, "state.txt");
  }

  public static ReplicationState getState(ReplicationEndpoint endpoint, int sequenceNumber)
      throws IOException, InterruptedException {
    var statePath = format("%s.state.txt", sequenceformatter.format(sequenceNumber));
    return getState(endpoint, statePath);
  }

  private static ReplicationState getState(ReplicationEndpoint endpoint, String statePath)
      throws IOException, InterruptedException {
    var request = HttpRequest.newBuilder(create(endpoint.url() + statePath)).GET().build();
    var response = httpClient.send(request, ofInputStream());
    var props = new Properties();
    try (var input = response.body()){
      props.load(input);
    }
    return new ReplicationState(endpoint, props);
  }

  private final ReplicationEndpoint endpoint;
  private final ZonedDateTime timestamp;
  private final int sequenceNumber;

  private ReplicationState(ReplicationEndpoint endpoint, Properties props) {
    this(endpoint,
        ZonedDateTime.parse(props.getProperty("timestamp")),
        parseInt(props.getProperty("sequenceNumber")));
  }

  public ReplicationState(ReplicationEndpoint endpoint, ZonedDateTime timestamp, int sequenceNumber) {
    this.endpoint = endpoint;
    this.timestamp = timestamp;
    this.sequenceNumber = sequenceNumber;
  }

  public ReplicationEndpoint getEndpoint() {
    return endpoint;
  }

  public ReplicationState serverState() throws IOException, InterruptedException {
    return endpoint.serverState();
  }

  public ReplicationState state(int sequenceNumber) throws IOException, InterruptedException {
    return endpoint.state(sequenceNumber);
  }

  public ZonedDateTime nextTimestamp() {
    return endpoint.nextTimestamp(this);
  }

  @Override
  public String getBaseUrl() {
    return endpoint.url();
  }

  @Override
  public ZonedDateTime getTimestamp() {
    return timestamp;
  }

  @Override
  public int getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public Flux<Tuple2<OSMType, Flux<OSMEntity>>> entities(TagTranslator tagTranslator) {
    Log.debug("read entities from {}", this);
    //noinspection UnstableApiUsage
    return using(this::openStream, input -> OscParser.entities(input, tagTranslator), Closeables::closeQuietly);
  }

  private InputStream openStream() throws IOException, InterruptedException {
    var request = HttpRequest.newBuilder(
            create(endpoint.url() + format("%s.osc.gz", sequenceformatter.format(sequenceNumber))))
        .GET().build();
    var response = httpClient.send(request, ofInputStream());
    return new GZIPInputStream(response.body());

  }

  @Override
  public String toString() {
    return format("ReplicationFile [endpoint=%s, timestamp=%s, sequenceNumber=%s]", endpoint,
        timestamp, sequenceNumber);
  }
}
