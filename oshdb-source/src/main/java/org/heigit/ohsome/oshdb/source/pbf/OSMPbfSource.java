package org.heigit.ohsome.oshdb.source.pbf;

import static java.util.stream.Collectors.groupingBy;
import static reactor.core.publisher.Flux.using;
import static reactor.core.scheduler.Schedulers.newParallel;

import com.google.common.io.Closeables;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.source.OSMSource;
import org.heigit.ohsome.oshdb.source.SourceUtil;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Scheduler;
import reactor.util.function.Tuple2;

public class OSMPbfSource implements OSMSource {

  private final Path path;

  public OSMPbfSource(Path path) {
    this.path = path;
  }

  @Override
  public Flux<Tuple2<OSMType, Flux<OSMEntity>>> entities(TagTranslator tagTranslator) {
    //noinspection UnstableApiUsage
    return using(this::openSource, source -> entities(source, tagTranslator), Closeables::closeQuietly);
  }

  private InputStream openSource() throws IOException {
    return Files.newInputStream(path);
  }

  private Flux<Tuple2<OSMType, Flux<OSMEntity>>> entities(InputStream source, TagTranslator tagTranslator) {
    return Flux.using(() -> newParallel("io"),
        scheduler -> blobs(source)
            .filter(Blob::isData)
            .flatMapSequential(blob -> blob.block().flatMapMany(block -> entities(block, tagTranslator)).subscribeOn(scheduler)),
        Scheduler::dispose);
  }

  private Flux<Tuple2<OSMType, Flux<OSMEntity>>> entities(Block block, TagTranslator tagTranslator) {
    var entities = block.entities().collect(groupingBy(OSMEntity::getType));
    return SourceUtil.entities(entities, block.tags(), block.roles(), tagTranslator);
  }

  private Flux<Blob> blobs(InputStream source) {
    return Flux.generate(sink -> readBlob(source, sink));
  }

  private static void readBlob(InputStream source, SynchronousSink<Blob> sink) {
    try {
      sink.next(Blob.read(source));
    } catch (EOFException e) {
      sink.complete();
    } catch (IOException e) {
      sink.error(e);
    }
  }
}
