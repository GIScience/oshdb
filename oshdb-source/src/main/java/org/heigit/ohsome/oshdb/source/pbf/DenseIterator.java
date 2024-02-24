package org.heigit.ohsome.oshdb.source.pbf;

import static java.lang.Boolean.TRUE;
import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyList;
import static org.heigit.ohsome.oshdb.osm.OSM.node;

import crosby.binary.Osmformat;
import crosby.binary.Osmformat.DenseNodes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;

class DenseIterator implements Iterator<OSMEntity> {

  private final Block block;
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

  public DenseIterator(Block block, Osmformat.DenseNodes dense) {
    this.block = block;
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
      keysVals = x -> emptyList();
    } else {
      this.keysVals = buildKeyVals(dense)::get;
    }
  }

  private List<List<OSHDBTag>> buildKeyVals(DenseNodes dense) {
    var list = new ArrayList<List<OSHDBTag>>(dense.getIdCount());
    var tags = new ArrayList<OSHDBTag>();
    var i = 0;
    while (i < dense.getKeysValsCount()) {
      var key = dense.getKeysVals(i++);
      if (key == 0) {
        block.addToBlockTags(tags);
        list.add(List.copyOf(tags));
        tags.clear();
      } else {
        var val = dense.getKeysVals(i++);
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
    return node(id, version, timestamp, changeset, user, tags, toIntExact(lon), toIntExact(lat));
  }
}
