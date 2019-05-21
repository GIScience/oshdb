package org.heigit.bigspatialdata.updater.OSCHandling;

import io.reactivex.Flowable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import org.heigit.bigspatialdata.updater.util.IteratorTmpl;
import org.heigit.bigspatialdata.updater.util.ReplicationFile;
import org.heigit.bigspatialdata.updater.util.XmlChangeReaderIterator;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.xml.common.CompressionActivator;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OSCParser extends IteratorTmpl<ChangeContainer> {

  private static final Logger LOG = LoggerFactory.getLogger(OSCParser.class);

  private Iterator<ChangeContainer> currChangeIterator;
  private ReplicationFile currFile;
  private final Iterator<ReplicationFile> replicationFiles;

  private OSCParser(Iterator<ReplicationFile> replicationFiles) {
    this.replicationFiles = replicationFiles;
  }

  /**
   * Represents the transform-step in an etl pipeline.
   *
   * @param replicationFiles
   * @return
   */
  public static Iterable<ChangeContainer> parse(Iterable<ReplicationFile> replicationFiles) {
    return () -> new OSCParser(replicationFiles.iterator());
  }

  /**
   * generate flow for ChangeContainer within a osc file
   */
  private static Flowable<ChangeContainer> generateChangeFlow(File osc) throws FileNotFoundException {
    InputStream inputStream = new CompressionActivator(CompressionMethod.GZip)
        .createCompressionInputStream(new FileInputStream(osc));
    Flowable<ChangeContainer> flow = Flowable.generate(() -> XmlChangeReaderIterator.of(
        inputStream),
        (reader, emitter) -> {
      if (reader.hasNext()) {
        emitter.onNext(reader.next());
      } else {
        if (reader.hasException()) {
          emitter.onError(reader.getException());
        } else {
          emitter.onComplete();
        }
      }
    }, (reader) -> reader.close());
    return flow;
  }

  private static Iterable<ChangeContainer> getChangeContainers(ReplicationFile replicationFile)
      throws FileNotFoundException {
    LOG.info(replicationFile.state + " -> " + replicationFile.file);
    return generateChangeFlow(replicationFile.file).blockingIterable();
  }

  @Override
  protected ChangeContainer getNext() throws Exception {
    if (currChangeIterator != null && currChangeIterator.hasNext()) {
      return currChangeIterator.next();
    } else if (replicationFiles.hasNext()) {
      currFile = replicationFiles.next();
      currChangeIterator = OSCParser.getChangeContainers(currFile).iterator();
      OSCDownloader.updateState(currFile.state.store());
      currFile.file.delete();
      return this.getNext();
    } else {
      return null;
    }

  }

}
