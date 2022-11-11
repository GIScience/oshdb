package org.heigit.ohsome.oshdb.osm.pbf;

import static java.util.stream.Collectors.toList;

import com.google.common.collect.Maps;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.TagTranslator;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMSource;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class OSMPbfSource implements OSMSource {

  private final Path path;

  public OSMPbfSource(Path path) {
    this.path = path;
  }

  @Override
  public Flux<OSMEntity> entities(TagTranslator translator) {
    return Flux.using(this::openSource,
        source -> entities(source, translator),
        this::closeQuietly);
  }

  private InputStream openSource() throws IOException {
    return Files.newInputStream(path);
  }

  private void closeQuietly(InputStream input) {
    try {
      input.close();
    } catch (IOException e) {
      // ignite ioexception
    }
  }

  private Flux<OSMEntity> entities(InputStream source, TagTranslator tagTranslator) {
    return Flux.using(() -> Schedulers.newParallel("io"),
        scheduler -> blobs(source)
            .filter(Blob::isData)
            .flatMapSequential(blob -> entities(blob, tagTranslator).subscribeOn(scheduler)),
        Scheduler::dispose);
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

  private Flux<OSMEntity> entities(Blob blob, TagTranslator translator) {
    return blob.block().flatMapMany(block -> entities(block, translator));
  }

  private Flux<OSMEntity> entities(Block block, TagTranslator translator) {
    var entities = block.entities();
    var tags = mapTags(block, translator);
    var roles = mapRoles(block, translator);
    return Flux.fromIterable(entities)
        .map(list -> rebuild(list, tags, roles))
        .concatMap(Flux::fromStream);
  }

  private Map<OSHDBTag, OSHDBTag> mapTags(Block block, TagTranslator translator) {
    var tags = block.getBlockTags();
    var translated = translator.getOSHDBTagOf(tags.values());
    var mapping = Maps.<OSHDBTag, OSHDBTag>newHashMapWithExpectedSize(tags.size());
    tags.forEach((oshdb, osm) -> mapping.put(oshdb, translated.get(osm)));
    return mapping;
  }

  private Map<OSHDBRole, OSHDBRole> mapRoles(Block block, TagTranslator translator) {
    var roles = block.getBlockRoles();
    var translated = translator.getOSHDBRoleOf(roles.values());
    var mapping = Maps.<OSHDBRole, OSHDBRole>newHashMapWithExpectedSize(roles.size());
    roles.forEach((oshdb, osm) -> mapping.put(oshdb, translated.get(osm)));
    return mapping;
  }

  private Stream<OSMEntity> rebuild(List<OSMEntity> list, Map<OSHDBTag, OSHDBTag> mappingTags,
      Map<OSHDBRole, OSHDBRole> mappingRoles) {
    var type = list.get(0).getType();
    switch (type) {
      case NODE:
        return rebuild(list, OSMNode.class, osm -> rebuild(osm, mappingTags));
      case WAY:
        return rebuild(list, OSMWay.class, osm -> rebuild(osm, mappingTags));
      case RELATION:
        return rebuild(list, OSMRelation.class, osm -> rebuild(osm, mappingTags, mappingRoles));
      default:
        throw new IllegalStateException();
    }
  }

  private <T extends OSMEntity> Stream<OSMEntity> rebuild(List<OSMEntity> list, Class<T> type,
      UnaryOperator<T> rebuild) {
    return list.stream().map(type::cast).map(rebuild);
  }

  private OSMNode rebuild(OSMNode osm, Map<OSHDBTag, OSHDBTag> mappingTags) {
    return rebuild(osm, mappingTags, (id, version, timestamp, changeset, user, tags) ->
        OSM.node(id, version, timestamp, changeset, user, tags, osm.getLon(), osm.getLat()));
  }

  private OSMWay rebuild(OSMWay osm, Map<OSHDBTag, OSHDBTag> mappingTags) {
    return rebuild(osm, mappingTags, (id, version, timestamp, changeset, user, tags) ->
        OSM.way(id, version, timestamp, changeset, user, tags, osm.getMembers()));
  }

  private OSMRelation rebuild(OSMRelation osm, Map<OSHDBTag, OSHDBTag> mappingTags,
      Map<OSHDBRole, OSHDBRole> mappingRoles) {
    return rebuild(osm, mappingTags, (id, version, timestamp, changeset, user, tags) ->
        OSM.relation(id, version, timestamp, changeset, user, tags,
            rebuildMembers(osm, mappingRoles)));
  }

  private OSMMember[] rebuildMembers(OSMRelation osm, Map<OSHDBRole, OSHDBRole> mappingRoles) {
    var members = osm.getMembers();
    for (var i = 0; i < members.length; i++) {
      var member = members[i];
      var id = member.getId();
      var type = member.getType();
      var role = mappingRoles.get(member.getRole());
      members[i] = new OSMMember(id, type, role.getId());
    }
    return members;
  }

  private <T extends OSMEntity> T rebuild(T entity, Map<OSHDBTag, OSHDBTag> mappingTags,
      EntityCommon<T> entityCommon) {
    var id = entity.getId();
    var visible = entity.isVisible() ? 1 : -1;
    var version = entity.getVersion() * visible;
    var timestamp = entity.getEpochSecond();
    var changeset = entity.getChangesetId();
    var user = entity.getUserId();
    var tags = entity.getTags().stream().map(mappingTags::get).sorted().collect(toList());
    return entityCommon.apply(id, version, timestamp, changeset, user, tags);
  }

  private interface EntityCommon<T> {
    T apply(long id, int version, long timestamp, long changeset, int user, List<OSHDBTag> tags);
  }
}
