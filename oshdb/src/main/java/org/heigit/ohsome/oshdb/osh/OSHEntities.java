package org.heigit.ohsome.oshdb.osh;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;

public abstract class OSHEntities {

  public static <T extends OSMEntity> List<T> toList(Iterable<T> versions) {
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

  public static OSMEntity getByTimestamp(OSHEntity osh, OSHDBTimestamp timestamp) {
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
}
