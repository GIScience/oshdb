package org.heigit.ohsome.oshdb.store.update;


import static org.heigit.ohsome.oshdb.osm.OSMType.NODE;
import static org.heigit.ohsome.oshdb.osm.OSMType.RELATION;
import static org.heigit.ohsome.oshdb.osm.OSMType.WAY;

import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.store.OSHDBStore;
import org.heigit.ohsome.oshdb.store.OSHData;
import org.heigit.ohsome.oshdb.util.CellId;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

public class OSHDBUpdater {

  private final OSHDBStore store;
  private final List<GridUpdater> gridUpdaters;

  private final Map<OSMType, Set<Long>> minorUpdates = new EnumMap<>(OSMType.class);
  private final Map<OSMType, Set<Long>> updatedEntities = new EnumMap<>(OSMType.class);

  public OSHDBUpdater(OSHDBStore store, List<GridUpdater> gridUpdaters) {
    this.store = store;
    this.gridUpdaters = gridUpdaters;
  }

  public Flux<OSHEntity> updateEntities(Flux<Tuple2<OSMType, Flux<OSMEntity>>> entities) {
    throw new UnsupportedOperationException();
  }

  public Flux<OSHEntity> entities(OSMType type, Flux<OSMEntity> entities) {
    return switch (type) {
      case NODE -> nodes(entities.cast(OSMNode.class));
      case WAY -> ways(entities.cast(OSMWay.class));
      case RELATION -> relations(entities.cast(OSMRelation.class));
    };
  }

  private Flux<OSHEntity> nodes(Flux<OSMNode> entities) {
    return entities.collectMultimap(OSMEntity::getId)
        .flatMapMany(this::nodes)
        .filter(Objects::nonNull)
        .map(OSHData::getOSHEntity);
  }

  private Flux<OSHData> nodes(Map<Long, Collection<OSMNode>> entities) {
    return Flux.using(() -> new Updates(store, minorUpdates, updatedEntities),
        updates -> updates.nodes(entities),
        updates -> updateGrid(NODE, updates.getGridUpdates()));
  }

  private Flux<OSHEntity> ways(Flux<OSMWay> entities) {
    return entities.collectMultimap(OSMEntity::getId)
        .flatMapMany(this::ways)
        .filter(Objects::nonNull)
        .map(OSHData::getOSHEntity);
  }

  private Flux<OSHData> ways(Map<Long, Collection<OSMWay>> entities) {
    return Flux.using(() -> new Updates(store, minorUpdates, updatedEntities),
        updates -> updates.ways(entities),
        updates -> updateGrid(WAY, updates.getGridUpdates()));
  }

  private Flux<OSHEntity> relations(Flux<OSMRelation> entities) {
    return entities.collectMultimap(OSMEntity::getId)
        .flatMapMany(this::relations)
        .filter(Objects::nonNull)
        .map(OSHData::getOSHEntity);
  }

  private Flux<OSHData> relations(Map<Long, Collection<OSMRelation>> entities) {
    return Flux.using(() -> new Updates(store, minorUpdates, updatedEntities),
        updates -> updates.relations(entities),
        updates -> updateGrid(RELATION, updates.getGridUpdates()));
  }

  private void updateGrid(OSMType type, Set<CellId> cellIds) {
    throw new UnsupportedOperationException();
  }

}
