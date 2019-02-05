package org.heigit.bigspatialdata.updater.OSCHandling;

import io.reactivex.Flowable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.util.Map;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.updater.util.ReplicationFile;
import org.heigit.bigspatialdata.updater.util.StateIterator;
import org.openstreetmap.osmosis.core.OsmosisConstants;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.util.PropertiesPersister;
import org.openstreetmap.osmosis.replication.common.ReplicationSequenceFormatter;
import org.openstreetmap.osmosis.replication.common.ReplicationState;
import org.openstreetmap.osmosis.replication.common.ServerStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSCDownloader {

  //TODO: warum zwischenstand in pbf schreiben? warum iterable in flowable in iterable und nicht iterator direkt? wegen state und mehreren Files? Wo ist unser Vorteil mit dem Iterable, wenn wir dann doch for-each machen?
  private static final String LOCAL_STATE_FILE = "update_state.txt";
  private static final Logger LOG = LoggerFactory.getLogger(OSCDownloader.class);
  private static final ReplicationSequenceFormatter sequenceFormatter = new ReplicationSequenceFormatter(9, 3);

  private static URL baseUrl;
  private static Path workingDirectory;
  private static PropertiesPersister localStatePersistor;

  /**
   *
   * @return
   */
  public static Iterable<ReplicationFile> download(URL baseURL, Path workingDirectory) {
    OSCDownloader.baseUrl = baseURL;
    OSCDownloader.workingDirectory = workingDirectory;
    OSCDownloader.localStatePersistor = new PropertiesPersister(workingDirectory.resolve(LOCAL_STATE_FILE).toFile());

    return OSCDownloader.generateStateFlow(OSCDownloader.getState()).map((ReplicationState state) -> {
      final String fileName = OSCDownloader.sequenceFormatter.getFormattedName(state.getSequenceNumber(), ".osc.gz");
      final File replicationFile = OSCDownloader.downloadReplicationFile(fileName);
      return new ReplicationFile(state, replicationFile);
    })
        // we could limit how many file we would like to process in this run!
        // just comment out this limit if you don't want it.
        //.limit(4)
        .blockingIterable();
  }

  /**
   * Represents the extract-step in an ETL pipeline.
   *
   * @return
   */
  public static Iterable<ReplicationFile> download(URL baseURL, Path workingDirectory, OSHDBTimestamp startTimestamp) {
    //TODO if possible, directly create Start-state from Timestamp so one does not have to search for state manually
    throw new AbstractMethodError();
  }

  public static void updateState(Map<String, String> state) {
    OSCDownloader.localStatePersistor.store(state);
  }

  private static ReplicationState getState() {
    final ServerStateReader serverStateReader = new ServerStateReader();
    final ReplicationState serverState = serverStateReader.getServerState(OSCDownloader.baseUrl);
    LOG.info("latest server state form " + OSCDownloader.baseUrl + " -> " + serverState.toString());

    final ReplicationState localState;
    if (!localStatePersistor.exists()) {
      LOG.warn("no prior state.txt exist in " + workingDirectory + " starting from latest server state now");
      localState = serverStateReader.getServerState(OSCDownloader.baseUrl, serverState.getSequenceNumber() - 1);
      localStatePersistor.store(localState.store());
    } else {

      localState = new ReplicationState(localStatePersistor.loadMap());
      LOG.info("latest local state -> " + localState.toString());
    }
    return localState;
  }

  /**
   * Download osc file from server
   */
  private static File downloadReplicationFile(String fileName) {
    URL changesetUrl;
    try {
      changesetUrl = new URL(OSCDownloader.baseUrl, fileName);
    } catch (MalformedURLException e) {
      throw new OsmosisRuntimeException("The server file URL could not be created.", e);
    }

    try {
      File outputFile;
      // Open an input stream for the changeset file on the server.
      URLConnection connection = changesetUrl.openConnection();
      connection.setReadTimeout(15 * 60 * 1000); // timeout 15 minutes
      connection.setConnectTimeout(15 * 60 * 1000); // timeout 15 minutes
      connection.setRequestProperty("User-Agent", "Osmosis/" + OsmosisConstants.VERSION);

      try (BufferedInputStream source = new BufferedInputStream(connection.getInputStream(), 65536)) {
        // Create a temporary file to write the data to.
        outputFile = File.createTempFile("change", null);

        // Open a output stream for the destination file.
        try (BufferedOutputStream sink = new BufferedOutputStream(new FileOutputStream(outputFile), 65536)) {
          // Download the file.
          byte[] buffer = new byte[65536];
          for (int bytesRead = source.read(buffer); bytesRead > 0; bytesRead = source.read(buffer)) {
            sink.write(buffer, 0, bytesRead);
          }
        }
      }

      return outputFile;

    } catch (IOException e) {
      throw new OsmosisRuntimeException("Unable to read the changeset file " + fileName + " from the server.", e);
    }
  }

  /**
   * generate flow for state retrieved from the server beginning from start state
   */
  private static Flowable<ReplicationState> generateStateFlow(ReplicationState startState) {
    Flowable<ReplicationState> flow = Flowable.generate(() -> new StateIterator(baseUrl, startState),
        (itr, emitter) -> {
          if (itr.hasNext()) {
            emitter.onNext(itr.next());
          } else {
            if (itr.hasException()) {
              emitter.onError(itr.getException());
            } else {
              emitter.onComplete();
            }
          }
        });
    return flow;
  }

}
