package org.heigit.ohsome.oshdb.util.osh;

import static java.lang.String.format;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;

public class OSHEntityTimeUtils {
  private OSHEntityTimeUtils() {
    throw new IllegalStateException("utility class");
  }

  /**
   * Returns the changeset ids which correspond to modifications of this entity.
   *
   * <p>Used internally to group modifications by changeset.</p>
   *
   * @param osh the osh entity to work on
   * @return a map between timestamps and changeset ids
   */
  public static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHEntity osh) {
    switch (osh.getType()) {
      case NODE:
        return getChangesetTimestamps((OSHNode) osh);
      case WAY:
        return getChangesetTimestamps((OSHWay) osh);
      case RELATION:
        return getChangesetTimestamps((OSHRelation) osh);
      default:
        throw new UnsupportedOperationException(format(UNSUPPORTED_OSMTYPE_MESSAGE, osh.getType()));
    }
  }

  private static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHNode osh) {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();
    osh.getVersions().forEach(osm -> result.put(osm.getTimestamp(), osm.getChangesetId()));
    return result;
  }

  private static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHWay osh) {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();

    List<OSMWay> ways = org.heigit.ohsome.oshdb.osh.OSHEntities.toList(osh.getVersions());
    ways.forEach(osm -> result.put(osm.getTimestamp(), osm.getChangesetId()));

    // recurse way nodes
    try {
      for (OSHNode node : osh.getNodes()) {
        if (node != null) {
          for (OSMNode version : node.getVersions()) {
            result.putIfAbsent(version.getTimestamp(), version.getChangesetId());
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return result;
  }

  private static Map<OSHDBTimestamp, Long> getChangesetTimestamps(OSHRelation osh) {
    Map<OSHDBTimestamp, Long> result = new TreeMap<>();

    List<OSMRelation> rels = org.heigit.ohsome.oshdb.osh.OSHEntities.toList(osh.getVersions());
    rels.forEach(osmRel -> result.put(osmRel.getTimestamp(), osmRel.getChangesetId()));

    // recurse rel members
    try {
      Stream.concat(osh.getNodes().stream(), osh.getWays().stream())
          .filter(Objects::nonNull)
          .forEach(oshEntity -> getChangesetTimestamps(oshEntity).forEach(result::putIfAbsent));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
    switch (osh.getType()) {
      case NODE:
        return getModificationTimestamps((OSHNode) osh);
      case WAY:
        return getModificationTimestamps((OSHWay) osh, true);
      case RELATION:
        return getModificationTimestamps((OSHRelation) osh, true);
      default:
        throw new UnsupportedOperationException(format(UNSUPPORTED_OSMTYPE_MESSAGE, osh.getType()));
    }
  }

  /**
   * Returns the list of timestamps at which this entity was modified.
   *
   * <p>
   * If the parameter "recurse" is set to true, it will also include modifications of the object's
   * child elements (useful to find out when the geometry of this object has been altered).
   * </p>
   *
   * @param osh the osh entity to work on
   * @param recurse specifies if times of modifications of child entities should also be returned or
   *        not
   * @return a list of timestamps where this entity has been modified
   */
  public static List<OSHDBTimestamp> getModificationTimestamps(OSHEntity osh, boolean recurse) {
    switch (osh.getType()) {
      case NODE:
        return getModificationTimestamps((OSHNode) osh);
      case WAY:
        return getModificationTimestamps((OSHWay) osh, recurse);
      case RELATION:
        return getModificationTimestamps((OSHRelation) osh, recurse);
      default:
        throw new UnsupportedOperationException(format(UNSUPPORTED_OSMTYPE_MESSAGE, osh.getType()));
    }
  }

  private static List<OSHDBTimestamp> getModificationTimestamps(OSHNode osh) {
    List<OSHDBTimestamp> result = new ArrayList<>();
    for (OSMEntity osm : osh.getVersions()) {
      result.add(osm.getTimestamp());
    }
    return Lists.reverse(result);
  }

  private static List<OSHDBTimestamp> getModificationTimestamps(OSHWay osh, boolean recurse) {
    return getModificationTimestamps(osh, recurse, null);
  }

  private static List<OSHDBTimestamp> getModificationTimestamps(OSHRelation osh, boolean recurse) {
    return getModificationTimestamps(osh, recurse, null);
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified and matches a given condition/filter.
   *
   * <p>
   * Consecutive modifications from a single changeset are grouped together (only the last
   * modification timestamp of the corresponding changeset is considered). This can reduce the
   * amount of geometry modifications by a lot (e.g. when sequential node uploads of a way
   * modification causes many intermediate modification states), making results more
   * "accurate"/comparable as well as allowing faster processing of geometries.
   * </p>
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
    switch (osh.getType()) {
      case NODE:
        allModificationTimestamps = getModificationTimestamps((OSHNode) osh, osmEntityFilter);
        break;
      case WAY:
        allModificationTimestamps = getModificationTimestamps((OSHWay) osh, osmEntityFilter);
        break;
      case RELATION:
        allModificationTimestamps = getModificationTimestamps((OSHRelation) osh, osmEntityFilter);
        break;
      default:
        throw new UnsupportedOperationException(format(UNSUPPORTED_OSMTYPE_MESSAGE, osh.getType()));
    }

    if (allModificationTimestamps.size() <= 1) {
      return allModificationTimestamps;
    }
    // group modification timestamps by changeset
    List<OSHDBTimestamp> result = new ArrayList<>(allModificationTimestamps.size());
    allModificationTimestamps = Lists.reverse(allModificationTimestamps);
    long nextChangeset = -1L;
    for (OSHDBTimestamp timestamp : allModificationTimestamps) {
      long changeset = changesetTimestamps.get(timestamp);
      if (changeset != nextChangeset) {
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
    switch (osh.getType()) {
      case NODE:
        return getModificationTimestamps((OSHNode) osh, osmEntityFilter);
      case WAY:
        return getModificationTimestamps((OSHWay) osh, osmEntityFilter);
      case RELATION:
        return getModificationTimestamps((OSHRelation) osh, osmEntityFilter);
      default:
        throw new UnsupportedOperationException(format(UNSUPPORTED_OSMTYPE_MESSAGE, osh.getType()));
    }
  }

  private static List<OSHDBTimestamp> getModificationTimestamps(OSHNode osh,
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

  private static List<OSHDBTimestamp> getModificationTimestamps(OSHWay osh,
      Predicate<OSMEntity> osmEntityFilter) {
    return getModificationTimestamps(osh, true, osmEntityFilter);
  }

  private static List<OSHDBTimestamp> getModificationTimestamps(OSHRelation osh,
      Predicate<OSMEntity> osmEntityFilter) {
    return getModificationTimestamps(osh, true, osmEntityFilter);
  }

  private static List<OSHDBTimestamp> getModificationTimestamps(OSHWay osh, boolean recurse,
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
        if (oshNode == null) {
          continue;
        }
        LinkedList<OSHDBTimestamp> childEntityValidityTimestamps;
        if (!childEntityTs.containsKey(oshNode)) {
          childEntityValidityTimestamps = new LinkedList<>();
          childEntityTs.put(oshNode, childEntityValidityTimestamps);
        } else {
          childEntityValidityTimestamps = childEntityTs.get(oshNode);
        }
        if (!childEntityValidityTimestamps.isEmpty()
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
          if (!modTs.hasNext()) {
            break outerTLoop;
          }
          current = modTs.next();
        }
        while (current.compareTo(toTs) <= 0) {
          result.add(current);
          if (!modTs.hasNext()) {
            break outerTLoop;
          }
          current = modTs.next();
        }
      }
    }

    return new ArrayList<>(result);
  }

  private static List<OSHDBTimestamp> getModificationTimestamps(OSHRelation osh, boolean recurse,
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
      if (!osmRelation.isVisible()
          || (osmEntityFilter != null && !osmEntityFilter.test(osmRelation))) {
        nextT = thisT;
        continue;
      }
      for (OSMMember member : osmRelation.getMembers()) {
        switch (member.getType()) {
          case NODE:
          case WAY:
            OSHEntity oshEntity = member.getEntity();
            if (oshEntity == null) {
              continue;
            }
            LinkedList<OSHDBTimestamp> childEntityValidityTimestamps;
            if (!childEntityTs.containsKey(oshEntity)) {
              childEntityValidityTimestamps = new LinkedList<>();
              childEntityTs.put(oshEntity, childEntityValidityTimestamps);
            } else {
              childEntityValidityTimestamps = childEntityTs.get(oshEntity);
            }
            if (!childEntityValidityTimestamps.isEmpty()
                && childEntityValidityTimestamps.getFirst().equals(nextT)) {
              // merge consecutive time intervals
              childEntityValidityTimestamps.pop();
              childEntityValidityTimestamps.push(thisT);
            } else {
              childEntityValidityTimestamps.push(nextT);
              childEntityValidityTimestamps.push(thisT);
            }
            break;
          default:
          case RELATION:
            // skip relation->relation members
            break;
        }
      }
      nextT = thisT;
    }

    SortedSet<OSHDBTimestamp> result = new TreeSet<>(relTs);

    for (Entry<OSHEntity, LinkedList<OSHDBTimestamp>> childEntityT : childEntityTs.entrySet()) {
      Iterator<OSHDBTimestamp> modTs = getModificationTimestamps(childEntityT.getKey()).iterator();
      if (!modTs.hasNext()) {
        // skip if the member has no "visible" version (for example because of data redactions)
        // see https://github.com/GIScience/oshdb/issues/325
        continue;
      }
      LinkedList<OSHDBTimestamp> validMemberTs = childEntityT.getValue();
      OSHDBTimestamp current = modTs.next();
      outerTLoop: while (!validMemberTs.isEmpty()) {
        OSHDBTimestamp fromTs = validMemberTs.pop();
        OSHDBTimestamp toTs = validMemberTs.pop();
        while (current.compareTo(fromTs) < 0) {
          if (!modTs.hasNext()) {
            break outerTLoop;
          }
          current = modTs.next();
        }
        while (current.compareTo(toTs) <= 0) {
          result.add(current);
          if (!modTs.hasNext()) {
            break outerTLoop;
          }
          current = modTs.next();
        }
      }
    }

    return new ArrayList<>(result);
  }

  private static final String UNSUPPORTED_OSMTYPE_MESSAGE = "cannot get timestamps for %s objects";
}
