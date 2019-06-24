package org.heigit.bigspatialdata.updater.oschandling;

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
import org.heigit.bigspatialdata.updater.util.replication.ReplicationFile;
import org.heigit.bigspatialdata.updater.util.replication.StateIterator;
import org.openstreetmap.osmosis.core.OsmosisConstants;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.util.PropertiesPersister;
import org.openstreetmap.osmosis.replication.common.ReplicationSequenceFormatter;
import org.openstreetmap.osmosis.replication.common.ReplicationState;
import org.openstreetmap.osmosis.replication.common.ServerStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provided static methods to download OSC-Replicafiles.
 */
public class OscDownloader {

  public static final String LOCAL_STATE_FILE = "update_state.txt";
  private static final Logger LOG = LoggerFactory.getLogger(OscDownloader.class);

  private static URL baseUrl;
  private static PropertiesPersister localStatePersistor;
  private static final ReplicationSequenceFormatter sequenceFormatter
      = new ReplicationSequenceFormatter(9, 3);
  private static Path workingDirectory;

  private OscDownloader() {
  }

  /**
   * Download prelicas, the start of a processing-line.
   *
   * @param baseUrl The base URL to be used (e.g. https://planet.openstreetmap.org/replication/day/)
   * @param workingDirectory the directory the replication files will be stored in. Should hold an
   *     osmium state.txt otherwise the download will only request the latest replication file.
   * @return An iterable over all replication files starting at state.txt until now
   */
  public static Iterable<ReplicationFile> download(URL baseUrl, Path workingDirectory) {
    OscDownloader.baseUrl = baseUrl;
    OscDownloader.workingDirectory = workingDirectory;
    OscDownloader.localStatePersistor
        = new PropertiesPersister(workingDirectory.resolve(LOCAL_STATE_FILE).toFile());

    return OscDownloader
        .generateStateFlow(OscDownloader.getState())
        .map((ReplicationState state) -> {
          final String fileName = OscDownloader.sequenceFormatter
              .getFormattedName(state.getSequenceNumber(), ".osc.gz");
          final File replicationFile = OscDownloader.downloadReplicationFile(fileName);
          return new ReplicationFile(state, replicationFile);
        })
        .blockingIterable();
  }

  /**
   * Update the state.txt to download the next replication file.
   *
   * @param state State to be written to file
   */
  public static void updateState(Map<String, String> state) {
    OscDownloader.localStatePersistor.store(state);
  }

  /**
   * Download osc file from server.
   */
  private static File downloadReplicationFile(String fileName) {
    URL changesetUrl;
    try {
      changesetUrl = new URL(OscDownloader.baseUrl, fileName);
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
        try (BufferedOutputStream sink = new BufferedOutputStream(new FileOutputStream(outputFile),
            65536)) {
          // Download the file.
          byte[] buffer = new byte[65536];
          for (int bytesRead = source.read(buffer);
              bytesRead > 0;
              bytesRead = source.read(buffer)) {
            sink.write(buffer, 0, bytesRead);
          }
        }
      }

      return outputFile;

    } catch (IOException e) {
      throw new OsmosisRuntimeException(
          "Unable to read the changeset file " + fileName + " from the server.", e);
    }
  }

  /**
   * generate flow for state retrieved from the server beginning from start state
   */
  private static Flowable<ReplicationState> generateStateFlow(ReplicationState startState) {
    Flowable<ReplicationState> flow = Flowable.generate(() -> new StateIterator(baseUrl,
        startState),
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

  private static ReplicationState getState() {
    final ServerStateReader serverStateReader = new ServerStateReader();
    final ReplicationState serverState = serverStateReader.getServerState(OscDownloader.baseUrl);
    LOG.info("latest server state form " + OscDownloader.baseUrl + " -> " + serverState.toString());

    final ReplicationState localState;
    if (!localStatePersistor.exists()) {
      LOG.warn(
          "no prior state.txt exist in "
          + workingDirectory
          + " starting from latest server state now");
      localState = serverStateReader.getServerState(OscDownloader.baseUrl, serverState
          .getSequenceNumber() - 1);
      localStatePersistor.store(localState.store());
    } else {

      localState = new ReplicationState(localStatePersistor.loadMap());
      LOG.info("latest local state -> " + localState.toString());
    }
    return localState;
  }

}
