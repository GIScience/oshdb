package org.heigit.ohsome.oshdb.tools.create;

import static java.util.Optional.ofNullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundable;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.store.OSHData;
import org.heigit.ohsome.oshdb.util.CellId;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.util.function.Tuples;

public class OSHDBImport {

  private OSHDBStore store;

  public void importEntities(Flux<OSMEntity> entities) {
    entities.windowUntilChanged(OSMEntity::getType)
        .concatMap(wnd -> wnd.switchOnFirst(this::getTypeOfFirst));

  }

  private Flux<OSMEntity> getTypeOfFirst(Signal<? extends OSMEntity> signal, Flux<OSMEntity> entities) {
    var first = signal.hasValue() ? signal.get() : null;
    if (first == null) {
      return entities;
    }
    var type = first.getType();
    return entities;
  }

  private void process(OSMType type, Flux<OSMEntity> entities) {
     switch (type) {
       case NODE: return nodes(entities.cast(OSMNode.class).bufferUntilChanged(OSMEntity::getId));
     }
  }

  private void nodes(Flux<List<OSMNode>> entities) {
    entities.buffer(1000);

  }
  private void ways(Flux<List<OSMWay>> entities) {
    entities.buffer(1000);
  }

  private Flux<Object> ways(List<List<OSMWay>> batch) {
    var nodeIds = batch.stream()
        .flatMap(List::stream)
        .map(OSMWay::getMembers)
        .flatMap(Arrays::stream)
        .map(OSMMember::getId)
        .collect(Collectors.toSet());

    var nodes = store.entities(OSMType.NODE, nodeIds);
    for (var osh : batch) {
      var oshNodes = new TreeMap<Long, OSHNode>();
      osh.stream().map(OSMWay::getMembers).flatMap(Arrays::stream)
              .forEach(member -> oshNodes.computeIfAbsent(member.getId(), memId ->
                  ofNullable(nodes.get(memId))
                      .map(data -> (OSHNode) data.getOSHEntity())
                      .orElse(null)));
      OSHWayImpl.build(osh, oshNodes.values());
    }
  }

  private OSHData way(List<OSMWay> versions, Map<Long, OSHData> nodes) {
    var memberIds = new TreeSet<Long>();
    versions.stream().map(OSMWay::getMembers)
        .flatMap(Arrays::stream)
        .map(OSMMember::getId)
        .forEach(memberIds::add);
    var members = new ArrayList<OSHNode>(memberIds.size());
    memberIds.stream()
        .map(nodes::get)
        .filter(Objects::nonNull)
        .map(data -> (OSHNode) data.getOSHEntity())
        .forEach(members::add);
    var osh = OSHWayImpl.build(versions, members);
   gridId(osh.getBoundable());
  }


  private void relations(Flux<List<OSMRelation>> entities) {}


  private CellId gridId(OSHDBBoundable bbox) {
    var delta = Math.max(bbox.getMaxLongitude() - bbox.getMinLongitude(),
        bbox.getMaxLatitude() - bbox.getMinLatitude());

    return null;
  }

}
