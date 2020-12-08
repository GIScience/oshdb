package org.heigit.bigspatialdata.oshdb.osh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import com.google.common.collect.Lists;

public abstract class OSHEntities {
  
  public static <T extends OSMEntity> List<T> toList(Iterable<T> versions){
    Iterator<T> itr = versions.iterator();
    T last = itr.next();
    List<T> list = new ArrayList<T>(last.getVersion());
    list.add(last);
    while(itr.hasNext())
      list.add(itr.next());
    return list;
  }

  public static SortedMap<OSHDBTimestamp, OSMEntity> getByTimestamps(
      OSHEntity osh, List<OSHDBTimestamp> byTimestamps) {
    return getByTimestamps(osh.getVersions(), byTimestamps);
  }
  
  public static SortedMap<OSHDBTimestamp, OSMEntity> getByTimestamps(
      Iterable<? extends OSMEntity> versions, List<OSHDBTimestamp> byTimestamps) {
    SortedMap<OSHDBTimestamp, OSMEntity> result = new TreeMap<>();

    int i = byTimestamps.size() - 1;
    Iterator<? extends OSMEntity> itr = versions.iterator();
    while (itr.hasNext() && i >= 0) {
      OSMEntity osm = itr.next();
      if (osm.getTimestamp().getRawUnixTimestamp() > byTimestamps.get(i).getRawUnixTimestamp()) {
        continue;
      } else {
        while (i >= 0 && osm.getTimestamp().getRawUnixTimestamp() <= byTimestamps.get(i)
            .getRawUnixTimestamp()) {
          result.put(byTimestamps.get(i), osm);
          i--;
        }
      }
    }
    return result;
  }

  public static <OSM extends OSMEntity> Map<OSHDBTimestamp, OSM> getByTimestamps(
      Iterable<OSM> versions) { // todo: name of method?
    Map<OSHDBTimestamp, OSM> result = new TreeMap<>();
    for (OSM osm : versions) {
      result.put(osm.getTimestamp(), osm);
    }
    return result;
    // todo: replace with call to getBetweenTimestamps(-Infinity, Infinity):
    // return this.getBetweenTimestamps(Long.MIN_VALUE, Long.MAX_VALUE);
  }

  public static OSMEntity getByTimestamp(OSHEntity osh,
      OSHDBTimestamp timestamp) {
    return getByTimestamp(osh.getVersions(), timestamp);
  }
  
  public static OSMEntity getByTimestamp(Iterable<? extends OSMEntity> versions,
      OSHDBTimestamp timestamp) {
    for (OSMEntity osm : versions) {
      if (osm.getTimestamp().getRawUnixTimestamp() <= timestamp.getRawUnixTimestamp()) {
        return osm;
      }
    }
    return null;
  }

  public static <OSM extends OSMEntity> List<OSM> getBetweenTimestamps(Iterable<OSM> versions,
      final OSHDBTimestamp t1, final OSHDBTimestamp t2) {
    final long maxTimestamp = Math.max(t1.getRawUnixTimestamp(), t2.getRawUnixTimestamp());
    final long minTimestamp = Math.min(t1.getRawUnixTimestamp(), t2.getRawUnixTimestamp());

    List<OSM> result = new ArrayList<>();

    for (OSM osm : versions) {
      if (osm.getTimestamp().getRawUnixTimestamp() > maxTimestamp) {
        continue;
      }
      result.add(osm);
      if (osm.getTimestamp().getRawUnixTimestamp() < minTimestamp) {
        break;
      }
    }
    return result;
  }

  /**
   * Returns the changeset ids which correspond to modifications of this entity.
   *
   * Used internally to group modifications by changeset.
   *
   * @param osh the osh entity to work on
   * @return a map between timestamps and changeset ids
   */
  public static Map<OSHDBTimestamp, Long> getChangesetTimestamps(
      OSHEntity osh) {
    if (osh instanceof OSHWay) {
      return getChangesetTimestamps((OSHWay) osh);
    } else if (osh instanceof OSHRelation) {
      return getChangesetTimestamps((OSHRelation) osh);
    }
    return getChangesetTimestamps((OSHNode) osh);
  }

  static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHNode osh) {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();
    osh.getVersions().forEach(osm -> result.putIfAbsent(osm.getTimestamp(), osm.getChangesetId()));
    return result;
  }

  static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHWay osh) {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();

    List<OSMWay> ways = toList(osh.getVersions());
    ways.forEach(osm -> {
      result.put(osm.getTimestamp(), osm.getChangesetId());
    });

    // recurse way nodes
    try {
      osh.getNodes().forEach(oshNode -> {
        if (oshNode != null)
          oshNode.getVersions().forEach(
              osmNode -> result.putIfAbsent(osmNode.getTimestamp(), osmNode.getChangesetId()));
      });
    } catch (IOException e) {
    }

    return result;
  }

  static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHRelation osh) {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();

    List<OSMRelation> rels = toList(osh.getVersions());
    rels.forEach(osmRel -> {
      result.put(osmRel.getTimestamp(), osmRel.getChangesetId());
    });

    // recurse rel members
    try {
      Stream.concat(osh.getNodes().stream(), osh.getWays().stream()).forEach(oshEntity -> {
        if (oshEntity != null)
          OSHEntities.getChangesetTimestamps(oshEntity).forEach(result::putIfAbsent);
      });
    } catch (IOException e) {
    }

    return result;
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified.
   * 
   * @param osh the osh entity to work on
   * @return a list of timestamps where this entity has been modified
   */
  public static List<OSHDBTimestamp> getModificationTimestamps(OSHEntity osh) {
    if (osh instanceof OSHWay)
      return getModificationTimestamps((OSHWay) osh, true);
    if (osh instanceof OSHRelation)
      return getModificationTimestamps((OSHRelation) osh, true);
    return getModificationTimestamps((OSHNode) osh, true);
  }

  /**
   * Returns the list of timestamps at which this entity was modified.
   *
   * If the parameter "recurse" is set to true, it will also include modifications of the object's
   * child elements (useful to find out when the geometry of this object has been altered).
   *
   * @param osh the osh entity to work on
   * @param recurse specifies if times of modifications of child entities should also be returned or
   *        not
   * @return a list of timestamps where this entity has been modified
   */
  static List<OSHDBTimestamp> getModificationTimestamps(OSHEntity osh,
      boolean recurse) {
    if (osh instanceof OSHWay)
      return getModificationTimestamps((OSHWay) osh, recurse);
    if (osh instanceof OSHRelation)
      return getModificationTimestamps((OSHRelation) osh, recurse);
    return getModificationTimestamps((OSHNode) osh, recurse);
  }

  static List<OSHDBTimestamp> getModificationTimestamps(OSHNode osh,
      @SuppressWarnings({"unused", "SameParameterValue"}) boolean recurse) {
    List<OSHDBTimestamp> result = new ArrayList<>();
    for (OSMEntity osm : osh.getVersions()) {
      result.add(osm.getTimestamp());
    }
    return Lists.reverse(result);
  }

  static List<OSHDBTimestamp> getModificationTimestamps(OSHWay osh, boolean recurse) {
    return _getModificationTimestamps(osh, recurse, null);
  }

  static List<OSHDBTimestamp> getModificationTimestamps(OSHRelation osh, boolean recurse) {
    return _getModificationTimestamps(osh, recurse, null);
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified and matches a given condition/filter.
   *
   * Consecutive modifications from a single changeset are grouped together (only the last
   * modification timestamp of the corresponding changeset is considered). This can reduce the
   * amount of geometry modifications by a lot (e.g. when sequential node uploads of a way
   * modification causes many intermediate modification states), making results more
   * "accurate"/comparable as well as allowing faster processing of geometries.
   *
   * @param osh the osh entity to work on
   * @param osmEntityFilter only timestamps for which the entity matches this filter are returned
   * @param changesetTimestamps association between timestamps and changeset-ids, can be obtained
   *        from oshEntity by calling {@link #getChangesetTimestamps}.
   * @return a list of timestamps where this entity has been modified
   */
  public static List<OSHDBTimestamp> getModificationTimestamps(OSHEntity osh,
      Predicate<OSMEntity> osmEntityFilter, Map<OSHDBTimestamp, Long> changesetTimestamps) {
    List<OSHDBTimestamp> allModificationTimestamps;
    if (osh instanceof OSHWay) {
      allModificationTimestamps = getModificationTimestamps((OSHWay) osh, osmEntityFilter);
    } else if (osh instanceof OSHRelation) {
      allModificationTimestamps = getModificationTimestamps((OSHRelation) osh, osmEntityFilter);
    } else {
      allModificationTimestamps = getModificationTimestamps((OSHNode) osh, osmEntityFilter);
    }

    if (allModificationTimestamps.size() <= 1) {
      return allModificationTimestamps;
    }
    // group modification timestamps by changeset
    List<OSHDBTimestamp> result = new ArrayList<>();
    allModificationTimestamps = Lists.reverse(allModificationTimestamps);
    Long nextChangeset = -1L;
    for (OSHDBTimestamp timestamp : allModificationTimestamps) {
      Long changeset = changesetTimestamps.get(timestamp);
      if (!Objects.equals(changeset, nextChangeset)) {
        result.add(timestamp);
      }
      nextChangeset = changeset;
    }

    return Lists.reverse(result);
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified and matches a given condition/filter.
   * 
   * @param osh the osh entity to work on
   * @param osmEntityFilter only timestamps for which the entity matches this filter are returned
   * @return a list of timestamps where this entity has been modified
   */
  public static List<OSHDBTimestamp> getModificationTimestamps(OSHEntity osh,
      Predicate<OSMEntity> osmEntityFilter) {
    if (osh instanceof OSHWay) {
      return getModificationTimestamps((OSHWay) osh, osmEntityFilter);
    } else if (osh instanceof OSHRelation) {
      return getModificationTimestamps((OSHRelation) osh, osmEntityFilter);
    }
    return getModificationTimestamps((OSHNode) osh, osmEntityFilter);
  }

  static List<OSHDBTimestamp> getModificationTimestamps(OSHNode osh,
      Predicate<OSMEntity> osmEntityFilter) {
    List<OSHDBTimestamp> result = new ArrayList<>();
    OSHDBTimestamp prevNonmatch = null;
    for (OSMEntity osm : osh.getVersions()) {
      if (osm.isVisible() && (osmEntityFilter == null || osmEntityFilter.test(osm))) {
        if (prevNonmatch != null) {
          result.add(prevNonmatch);
          prevNonmatch = null;
        }
        result.add(osm.getTimestamp());
      } else {
        prevNonmatch = osm.getTimestamp();
      }
    }
    return Lists.reverse(result);
  }

  static List<OSHDBTimestamp> getModificationTimestamps(OSHWay osh,
      Predicate<OSMEntity> osmEntityFilter) {
    return _getModificationTimestamps(osh, true, osmEntityFilter);
  }

  static List<OSHDBTimestamp> getModificationTimestamps(OSHRelation osh,
      Predicate<OSMEntity> osmEntityFilter) {
    return _getModificationTimestamps(osh, true, osmEntityFilter);
  }

  private static List<OSHDBTimestamp> _getModificationTimestamps(OSHWay osh, boolean recurse,
      Predicate<OSMEntity> osmEntityFilter) {
    List<OSHDBTimestamp> wayTs = new ArrayList<>();
    List<OSMWay> versions = Lists.newLinkedList(osh.getVersions());
    OSHDBTimestamp prevNonmatch = null;
    for (OSMWay osm : versions) {
      if (osm.isVisible() && (osmEntityFilter == null || osmEntityFilter.test(osm))) {
        if (prevNonmatch != null) {
          wayTs.add(prevNonmatch);
          prevNonmatch = null;
        }
        wayTs.add(osm.getTimestamp());
      } else {
        prevNonmatch = osm.getTimestamp();
      }
    }
    if (!recurse) {
      return Lists.reverse(wayTs);
    }

    Map<OSHNode, LinkedList<OSHDBTimestamp>> childEntityTs = new TreeMap<>();

    OSHDBTimestamp nextT = new OSHDBTimestamp(Long.MAX_VALUE);
    for (OSMWay osm : versions) {
      OSHDBTimestamp thisT = osm.getTimestamp();
      if (!osm.isVisible() || (osmEntityFilter != null && !osmEntityFilter.test(osm))) {
        nextT = thisT;
        continue;
      }
      OSMMember[] nds = osm.getRefs();
      for (OSMMember nd : nds) {
        OSHNode oshNode = (OSHNode) nd.getEntity();
        if (oshNode == null)
          continue;
        LinkedList<OSHDBTimestamp> childEntityValidityTimestamps;
        if (!childEntityTs.containsKey(oshNode)) {
          childEntityValidityTimestamps = new LinkedList<>();
          childEntityTs.put(oshNode, childEntityValidityTimestamps);
        } else {
          childEntityValidityTimestamps = childEntityTs.get(oshNode);
        }
        if (childEntityValidityTimestamps.size() > 0
            && childEntityValidityTimestamps.getFirst().equals(nextT)) {
          // merge consecutive time intervals
          childEntityValidityTimestamps.pop();
          childEntityValidityTimestamps.push(thisT);
        } else {
          childEntityValidityTimestamps.push(nextT);
          childEntityValidityTimestamps.push(thisT);
        }
      }
      nextT = thisT;
    }

    SortedSet<OSHDBTimestamp> result = new TreeSet<>(wayTs);

    for (Entry<OSHNode, LinkedList<OSHDBTimestamp>> childEntityT : childEntityTs.entrySet()) {
      Iterator<OSHDBTimestamp> modTs = getModificationTimestamps(childEntityT.getKey()).iterator();
      LinkedList<OSHDBTimestamp> validMemberTs = childEntityT.getValue();
      OSHDBTimestamp current = modTs.next();
      outerTLoop: while (!validMemberTs.isEmpty()) {
        OSHDBTimestamp fromTs = validMemberTs.pop();
        OSHDBTimestamp toTs = validMemberTs.pop();
        while (current.compareTo(fromTs) < 0) {
          if (!modTs.hasNext())
            break outerTLoop;
          current = modTs.next();
        }
        while (current.compareTo(toTs) <= 0) {
          result.add(current);
          if (!modTs.hasNext())
            break outerTLoop;
          current = modTs.next();
        }
      }
    }

    return new ArrayList<>(result);
  }

  private static List<OSHDBTimestamp> _getModificationTimestamps(OSHRelation osh, boolean recurse,
      Predicate<OSMEntity> osmEntityFilter) {
    List<OSHDBTimestamp> relTs = new ArrayList<>();
    OSHDBTimestamp prevNonmatch = null;
    List<OSMRelation> versions = Lists.newLinkedList(osh.getVersions());
    for (OSMRelation osm : versions) {
      if (osm.isVisible() && (osmEntityFilter == null || osmEntityFilter.test(osm))) {
        if (prevNonmatch != null) {
          relTs.add(prevNonmatch);
          prevNonmatch = null;
        }
        relTs.add(osm.getTimestamp());
      } else {
        prevNonmatch = osm.getTimestamp();
      }
    }
    if (!recurse) {
      return Lists.reverse(relTs);
    }

    Map<OSHEntity, LinkedList<OSHDBTimestamp>> childEntityTs = new TreeMap<>();
    OSHDBTimestamp nextT = new OSHDBTimestamp(Long.MAX_VALUE);
    for (OSMRelation osmRelation : versions) {
      OSHDBTimestamp thisT = osmRelation.getTimestamp();
      if (!osmRelation.isVisible() ||
          (osmEntityFilter != null && !osmEntityFilter.test(osmRelation))) {
        nextT = thisT;
        continue;
      }
      for (OSMMember member : osmRelation.getMembers()) {
        switch (member.getType()) {
          case NODE:
          case WAY:
            OSHEntity oshEntity = member.getEntity();
            if (oshEntity == null)
              continue;
            LinkedList<OSHDBTimestamp> childEntityValidityTimestamps;
            if (!childEntityTs.containsKey(oshEntity)) {
              childEntityValidityTimestamps = new LinkedList<>();
              childEntityTs.put(oshEntity, childEntityValidityTimestamps);
            } else {
              childEntityValidityTimestamps = childEntityTs.get(oshEntity);
            }
            if (childEntityValidityTimestamps.size() > 0
                && childEntityValidityTimestamps.getFirst().equals(nextT)) {
              // merge consecutive time intervals
              childEntityValidityTimestamps.pop();
              childEntityValidityTimestamps.push(thisT);
            } else {
              childEntityValidityTimestamps.push(nextT);
              childEntityValidityTimestamps.push(thisT);
            }
          default:
            break;
        }
      }
      nextT = thisT;
    }

    SortedSet<OSHDBTimestamp> result = new TreeSet<>(relTs);

    for (Entry<OSHEntity, LinkedList<OSHDBTimestamp>> childEntityT : childEntityTs.entrySet()) {
      Iterator<OSHDBTimestamp> modTs = getModificationTimestamps(childEntityT.getKey()).iterator();
      LinkedList<OSHDBTimestamp> validMemberTs = childEntityT.getValue();
      OSHDBTimestamp current = modTs.next();
      outerTLoop: while (!validMemberTs.isEmpty()) {
        OSHDBTimestamp fromTs = validMemberTs.pop();
        OSHDBTimestamp toTs = validMemberTs.pop();
        while (current.compareTo(fromTs) < 0) {
          if (!modTs.hasNext())
            break outerTLoop;
          current = modTs.next();
        }
        while (current.compareTo(toTs) <= 0) {
          result.add(current);
          if (!modTs.hasNext())
            break outerTLoop;
          current = modTs.next();
        }
      }
    }

    return new ArrayList<>(result);
  }
}
