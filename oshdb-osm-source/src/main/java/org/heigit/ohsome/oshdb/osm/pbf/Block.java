package org.heigit.ohsome.oshdb.osm.pbf;

import static java.lang.Boolean.TRUE;
import static java.lang.Math.toIntExact;
import static java.util.Spliterators.spliterator;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

import com.google.protobuf.InvalidProtocolBufferException;
import crosby.binary.Osmformat;
import crosby.binary.Osmformat.DenseNodes;
import crosby.binary.Osmformat.PrimitiveGroup;
import crosby.binary.file.FileFormatException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
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
        throw new OSHDBException(
            "expected date granularity must be 1000! But got " + dateGranularity);
      }
      if (lonOffset != 0 || latOffset != 0) {
        throw new OSHDBException(
            "expected lon/lat offset must be 0! But got " + lonOffset + "/" + latOffset);
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
  private final Osmformat.PrimitiveBlock primitiveBlock;
  private final String[] strings;
  private final Map<OSHDBTag, OSMTag> blockTags = new HashMap<>();
  private final Map<OSHDBRole, OSMRole> blockRoles = new HashMap<>();

  private Block(Blob blob, Osmformat.PrimitiveBlock block, String[] strings) {
    this.blob = blob;
    this.primitiveBlock = block;
    this.strings = strings;
  }

  public Blob getBlob() {
    return blob;
  }

  public List<List<OSMEntity>> entities() {
    return primitiveBlock.getPrimitivegroupList().stream()
        .flatMap(this::groupToEntities)
        .filter(not(List::isEmpty))
        .collect(toList());
  }

  private Stream<List<OSMEntity>> groupToEntities(Osmformat.PrimitiveGroup group) {
    return Stream.of(
            denseToEntities(group),
            group.getNodesList().stream().map(this::parse),
            group.getWaysList().stream().map(this::parse),
            group.getRelationsList().stream().map(this::parse))
        .map(stream -> stream.collect(toList()));
  }

  private Stream<OSMEntity> denseToEntities(PrimitiveGroup group) {
    if (!group.hasDense()) {
      return Stream.empty();
    }
    var dense = group.getDense();
    var itr = new DenseIterator(dense);
    return stream(spliterator(itr, dense.getIdCount(), Spliterator.ORDERED), false);
  }

  public Map<OSHDBTag, OSMTag> getBlockTags() {
    return blockTags;
  }

  public Map<OSHDBRole, OSMRole> getBlockRoles() {
    return blockRoles;
  }

  private class DenseIterator implements Iterator<OSMEntity> {

    private final Osmformat.DenseNodes dense;

    private final List<Integer> versions;
    private final List<Long> timestamps;
    private final List<Long> changesets;
    private final List<Integer> users;
    private final IntFunction<Boolean> visibilities;
    private final IntFunction<List<OSHDBTag>> keysVals;

    private long id;
    private long timestamp;
    private long changeset;
    private int user;
    private long lon;
    private long lat;

    private int next = 0;

    public DenseIterator(Osmformat.DenseNodes dense) {
      this.dense = dense;
      if (!dense.hasDenseinfo()) {
        throw new OSHDBException("entity info is required for oshdb");
      }

      var info = dense.getDenseinfo();
      versions = info.getVersionList();
      timestamps = info.getTimestampList();
      changesets = info.getChangesetList();
      users = info.getUidList();
      if (!info.getVisibleList().isEmpty()) {
        visibilities = info.getVisibleList()::get;
      } else {
        visibilities = x -> true;
      }

      if (dense.getKeysValsList().isEmpty()) {
        keysVals = x -> Collections.emptyList();
      } else {
        this.keysVals = buildKeyVals(dense)::get;
      }
    }

    private List<List<OSHDBTag>> buildKeyVals(DenseNodes dense) {
      var list = new ArrayList<List<OSHDBTag>>(dense.getIdCount());
      var tags = new ArrayList<OSHDBTag>();
      for (var i = 0; i < dense.getKeysValsCount(); i++) {
        var key = dense.getKeysVals(i);
        if (key == 0) {
          addToBlockTags(tags);
          list.add(List.copyOf(tags));
          tags.clear();
        } else {
          var val = dense.getKeysVals(++i);
          tags.add(new OSHDBTag(key, val));
        }
      }
      return list;
    }

    @Override
    public boolean hasNext() {
      return next < dense.getIdCount();
    }

    @Override
    public OSMEntity next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return getNext(next++);
    }

    private OSMEntity getNext(int index) {
      id += dense.getId(index);
      timestamp += timestamps.get(index);
      changeset += changesets.get(index);
      user += users.get(index);

      var visible = TRUE.equals(visibilities.apply(index)) ? 1 : -1;
      var version = versions.get(index) * visible;

      var tags = keysVals.apply(index);
      lon += dense.getLon(index);
      lat += dense.getLat(index);
      return OSM.node(id, version, timestamp, changeset, user, tags, toIntExact(lon),
          toIntExact(lat));
    }
  }

  private OSMNode parse(Osmformat.Node entity) {
    var id = entity.getId();
    var lon = entity.getLon();
    var lat = entity.getLat();

    return withInfo(entity.getKeysList(), entity.getValsList(), entity.getInfo(),
        (timestamp, changeset, user, version, tags) ->
            OSM.node(id, version, timestamp, changeset, user, tags, toIntExact(lon),
                toIntExact(lat)));
  }

  private OSMWay parse(Osmformat.Way entity) {
    var id = entity.getId();
    var members = new OSMMember[entity.getRefsCount()];
    var memId = 0L;
    for (var i = 0; i < members.length; i++) {
      memId += entity.getRefs(i);
      members[i] = new OSMMember(memId, OSMType.NODE, -1);
    }
    return withInfo(entity.getKeysList(), entity.getValsList(), entity.getInfo(),
        (timestamp, changeset, user, version, tags) ->
            OSM.way(id, version, timestamp, changeset, user, tags, members));
  }

  private OSMRelation parse(Osmformat.Relation entity) {
    var id = entity.getId();
    var members = new OSMMember[entity.getMemidsCount()];
    var memId = 0L;
    var roles = new HashSet<OSHDBRole>();
    for (var i = 0; i < members.length; i++) {
      memId += entity.getMemids(i);
      var type = entity.getTypes(i);
      var role = entity.getRolesSid(i);
      var member =  new OSMMember(memId, OSMType.fromInt(type.getNumber()), role);
      roles.add(member.getRole());
      members[i] = member;
    }
    addToBlockRoles(roles);
    return withInfo(entity.getKeysList(), entity.getValsList(), entity.getInfo(),
        (timestamp, changeset, user, version, tags) ->
            OSM.relation(id, version, timestamp, changeset, user, tags, members));
  }


  private <T> T withInfo(List<Integer> keys, List<Integer> values, Osmformat.Info info,
      EntityInfo<T> metadata) {
    var timestamp = info.getTimestamp();
    var changeset = info.getChangeset();
    var user = info.getUid();

    var visible = info.hasVisible() && !info.getVisible() ? -1 : 1;
    var version = info.getVersion() * visible;

    var tags = new ArrayList<OSHDBTag>(keys.size());
    for (var i = 0; i < keys.size(); i++) {
      tags.add(new OSHDBTag(keys.get(i), values.get(i)));
    }
    addToBlockTags(tags);
    return metadata.apply(timestamp, changeset, user, version, tags);
  }

  private interface EntityInfo<T> {
    T apply(long timestamp, long changeset, int user, int version, List<OSHDBTag> tags);
  }

  private void addToBlockTags(List<OSHDBTag> tags) {
    tags.forEach(tag -> blockTags.computeIfAbsent(tag,this::toOSMTag));
  }

  private OSMTag toOSMTag(OSHDBTag tag) {
    return new OSMTag(strings[tag.getKey()], strings[tag.getValue()]);
  }

  private void addToBlockRoles(Set<OSHDBRole> roles) {
    roles.forEach(role -> blockRoles.computeIfAbsent(role, this::toOSMRole));
  }

  private OSMRole toOSMRole(OSHDBRole role) {
    return new OSMRole(strings[role.getId()]);
  }
}
