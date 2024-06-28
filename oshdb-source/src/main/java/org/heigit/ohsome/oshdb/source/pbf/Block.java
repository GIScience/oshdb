package org.heigit.ohsome.oshdb.source.pbf;

import static java.lang.Math.toIntExact;
import static java.util.Optional.ofNullable;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliterator;
import static java.util.stream.StreamSupport.stream;
import static org.heigit.ohsome.oshdb.osm.OSM.node;
import static org.heigit.ohsome.oshdb.osm.OSM.relation;
import static org.heigit.ohsome.oshdb.osm.OSM.way;
import static org.heigit.ohsome.oshdb.osm.OSMType.NODE;

import com.google.common.collect.Streams;
import com.google.protobuf.InvalidProtocolBufferException;
import crosby.binary.Osmformat;
import crosby.binary.Osmformat.PrimitiveBlock;
import crosby.binary.Osmformat.PrimitiveGroup;
import crosby.binary.file.FileFormatException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMRole;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;

public class Block {


  public static Block parse(Blob blob, byte[] data) throws FileFormatException {
    try {
      var block = Osmformat.PrimitiveBlock.parseFrom(data);

      var granularity = block.getGranularity();
      var latOffset = block.getLatOffset();
      var lonOffset = block.getLonOffset();
      var dateGranularity = block.getDateGranularity();

      if (granularity != 100) {
        throw new OSHDBException("expected granularity must be 100! But got " + granularity);
      }
      if (dateGranularity != 1000) {
        throw new OSHDBException("expected date granularity must be 1000! But got " + dateGranularity);
      }
      if (lonOffset != 0 || latOffset != 0) {
        throw new OSHDBException("expected lon/lat offset must be 0! But got " + lonOffset + "/" + latOffset);
      }

      var stringTable = block.getStringtable();
      var strings = new String[stringTable.getSCount()];
      for (int i = 0; i < strings.length; i++) {
        strings[i] = stringTable.getS(i).toStringUtf8();
      }
      return new Block(blob, block, strings);
    } catch (InvalidProtocolBufferException e) {
      throw new FileFormatException(e);
    }
  }

  private final Blob blob;
  private final PrimitiveBlock primitiveBlock;
  private final String[] strings;

  private final Map<OSHDBTag, OSMTag> tags = new HashMap<>();
  private final Map<Integer, OSMRole> roles = new HashMap<>();

  private Block(Blob blob, PrimitiveBlock block, String[] strings) {
    this.blob = blob;
    this.primitiveBlock = block;
    this.strings = strings;
  }

  @Override
  public String toString() {
    return "Block{blob=" + blob +'}';
  }

  public Stream<OSMEntity> entities() {
    return primitiveBlock.getPrimitivegroupList().stream()
        .flatMap(this::groupToEntities);
  }

  private Stream<OSMEntity> groupToEntities(Osmformat.PrimitiveGroup group) {
    return Streams.concat(
            denseToEntities(group),
            group.getNodesList().stream().map(this::parse),
            group.getWaysList().stream().map(this::parse),
            group.getRelationsList().stream().map(this::parse));
  }

  private Stream<OSMEntity> denseToEntities(PrimitiveGroup group) {
    if (!group.hasDense()) {
      return Stream.empty();
    }
    var dense = group.getDense();
    var itr = new DenseIterator(this, dense);
    return stream(spliterator(itr, dense.getIdCount(), ORDERED), false);
  }

  private OSMEntity parse(Osmformat.Node entity) {
    var id = entity.getId();
    var lon = entity.getLon();
    var lat = entity.getLat();

    return withInfo(entity.getKeysList(), entity.getValsList(), entity.getInfo(),
        (timestamp, changeset, user, version, tags) ->
            node(id, version, timestamp, changeset, user, tags, toIntExact(lon), toIntExact(lat)));
  }

  private OSMEntity parse(Osmformat.Way entity) {
    var id = entity.getId();
    var members = new OSMMember[entity.getRefsCount()];
    var memId = 0L;
    for (var i = 0; i < members.length; i++) {
      memId += entity.getRefs(i);
      members[i] = new OSMMember(memId, NODE, -1);
    }
    return withInfo(entity.getKeysList(), entity.getValsList(), entity.getInfo(),
        (timestamp, changeset, user, version, tags) ->
            way(id, version, timestamp, changeset, user, tags, members));
  }

  private final Map<OSMMember, OSMMember> memCache = new HashMap<>();

  private OSMEntity parse(Osmformat.Relation entity) {
    var id = entity.getId();
    var members = new OSMMember[entity.getMemidsCount()];
    var memId = 0L;
    var relationRoles = new HashSet<OSHDBRole>();
    for (var i = 0; i < members.length; i++) {
      memId += entity.getMemids(i);
      var type = entity.getTypes(i);
      var role = entity.getRolesSid(i);
      var member =  new OSMMember(memId, OSMType.fromInt(type.getNumber()), role);
      relationRoles.add(member.getRole());
      members[i] = ofNullable(memCache.putIfAbsent(member, member)).orElse(member);
    }
    addToBlockRoles(relationRoles);
    return withInfo(entity.getKeysList(), entity.getValsList(), entity.getInfo(),
        (timestamp, changeset, user, version, tags) ->
            relation(id, version, timestamp, changeset, user, tags, members));
  }

  private <T> T withInfo(List<Integer> keys, List<Integer> values, Osmformat.Info info,
      EntityInfo<T> metadata) {
    var timestamp = info.getTimestamp();
    var changeset = info.getChangeset();
    var user = info.getUid();

    var visible = info.hasVisible() && !info.getVisible() ? -1 : 1;
    var version = info.getVersion() * visible;

    var entityTags = new ArrayList<OSHDBTag>(keys.size());
    for (var i = 0; i < keys.size(); i++) {
      entityTags.add(new OSHDBTag(keys.get(i), values.get(i)));
    }
    addToBlockTags(entityTags);
    return metadata.apply(timestamp, changeset, user, version, entityTags);
  }

  private interface EntityInfo<T> {
    T apply(long timestamp, long changeset, int user, int version, List<OSHDBTag> tags);
  }

  void addToBlockTags(List<OSHDBTag> tags) {
    tags.forEach(tag -> this.tags.computeIfAbsent(tag,this::osmTag));
  }

  void addToBlockRoles(Set<OSHDBRole> roles) {
    roles.forEach(role -> this.roles.computeIfAbsent(role.getId(), this::osmRole));
  }

  private OSMTag osmTag(OSHDBTag tag) {
    return new OSMTag(strings[tag.getKey()], strings[tag.getValue()]);
  }

  private OSMRole osmRole(int role) {
    return new OSMRole(strings[role]);
  }

  public Map<OSHDBTag, OSMTag> tags() {
    return tags;
  }

  public Map<Integer, OSMRole> roles() {
    return roles;
  }
}
