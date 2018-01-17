package org.heigit.bigspatialdata.oshdb.osh;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.ObjectOutput;
import java.util.*;
import java.util.function.Predicate;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;

@SuppressWarnings("rawtypes")
public abstract class OSHEntity<OSM extends OSMEntity>
    implements Comparable<OSHEntity>, Iterable<OSM> {

  protected final byte[] data;
  protected final int offset;
  protected final int length;
  protected final long baseTimestamp;
  protected final long baseLongitude;
  protected final long baseLatitude;

  protected final long id;
  protected final byte header;
  protected final BoundingBox bbox;
  protected final int[] keys;
  protected final int dataOffset;
  protected final int dataLength;

  public OSHEntity(final byte[] data, final int offset, final int length, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final byte header, final long id, final BoundingBox bbox, final int[] keys,
      final int dataOffset, final int dataLength) {
    this.data = data;
    this.offset = offset;
    this.length = length;

    this.baseTimestamp = baseTimestamp;
    this.baseLongitude = baseLongitude;
    this.baseLatitude = baseLatitude;

    this.header = header;
    this.id = id;
    this.bbox = bbox;
    this.keys = keys;
    this.dataOffset = dataOffset;
    this.dataLength = dataLength;
  }

  public byte[] getData() {
    if (offset == 0 && length == data.length) {
      return data;
    }
    byte[] result = new byte[length];
    System.arraycopy(data, offset, result, 0, length);
    return result;
  }

  public long getId() {
    return id;
  }

  public abstract OSMType getType();

  public int getLength() {
    return length;
  }

  public BoundingBox getBoundingBox() {
    return bbox;
  }

  public int[] getKeys() {
    return keys;
  }

  public abstract List<OSM> getVersions();

  public OSM getLatest() {
    return iterator().next();
  }

  /* byTimestamps is assumed to be presorted, otherwise output is undetermined */
  public SortedMap<OSHDBTimestamp, OSM> getByTimestamps(List<OSHDBTimestamp> byTimestamps) {
    SortedMap<OSHDBTimestamp, OSM> result = new TreeMap<>();

    int i = byTimestamps.size() - 1;
    Iterator<OSM> itr = iterator();
    while (itr.hasNext() && i >= 0) {
      OSM osm = itr.next();
      if (osm.getTimestamp().getRawUnixTimestamp() > byTimestamps.get(i).getRawUnixTimestamp()) {
        continue;
      } else {
        while (i >= 0 && osm.getTimestamp().getRawUnixTimestamp() <= byTimestamps.get(i).getRawUnixTimestamp()) {
          result.put(byTimestamps.get(i), osm);
          i--;
        }
      }
    }
    return result;
  }

  public Map<OSHDBTimestamp, OSM> getByTimestamps() { // todo: name of method?
    Map<OSHDBTimestamp, OSM> result = new TreeMap<>();
    for (OSM osm : this) {
      result.put(osm.getTimestamp(), osm);
    }
    return result;
    // todo: replace with call to getBetweenTimestamps(-Infinity, Infinity):
    // return this.getBetweenTimestamps(Long.MIN_VALUE, Long.MAX_VALUE);
  }

  public OSM getByTimestamp(OSHDBTimestamp timestamp) {
    for (OSM osm : this) {
      if (osm.getTimestamp().getRawUnixTimestamp() <= timestamp.getRawUnixTimestamp()) {
        return osm;
      }
    }
    return null;
  }

  public List<OSM> getBetweenTimestamps(final OSHDBTimestamp t1, final OSHDBTimestamp t2) {
    final long maxTimestamp = Math.max(t1.getRawUnixTimestamp(), t2.getRawUnixTimestamp());
    final long minTimestamp = Math.min(t1.getRawUnixTimestamp(), t2.getRawUnixTimestamp());

    List<OSM> result = new ArrayList<>();

    for (OSM osm : this) {
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



  public boolean hasTagKey(int key) {
    // todo: replace with binary search (keys are sorted)
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] == key) {
        return true;
      }
      if (keys[i] > key) {
        break;
      }
    }
    return false;
  }

  public abstract OSHEntity<OSM> rebase(long baseId, long baseTimestamp, long baseLongitude,
      long baseLatitude) throws IOException;

  @Override
  public int compareTo(OSHEntity o) {
    int c = Long.compare(id, o.id);
    return c;
  }

  public void writeTo(ByteArrayOutputWrapper out) throws IOException {
    out.writeByteArray(data, offset, length);
  }

  public int writeTo(ObjectOutput out) throws IOException {
    out.write(data, offset, length);
    return length;
  }

  /**
   * returns true if the bbox of this HOSM entity intersects (or is fully inside) the given bbox.
   * Used to roughly pre-filter objects against a bbox.
   *
   * @param otherBbox the bounding box which this entity is tested against
   */
  public boolean intersectsBbox(BoundingBox otherBbox) {
    BoundingBox bbox = this.getBoundingBox();
    if (bbox == null) {
      return false;
    }
    if (bbox.maxLat < otherBbox.minLat) {
      return false;
    }
    if (bbox.minLat > otherBbox.maxLat) {
      return false;
    }
    if (bbox.maxLon < otherBbox.minLon) {
      return false;
    }
    if (bbox.minLon > otherBbox.maxLon) {
      return false;
    }
    return true;
  }

  /**
   * returns true if the bbox of this HOSM entity is fully inside the given bbox. Can be used as an
   * optimization to find not-to-be-clipped entity Geometries
   *
   * @param otherBbox the bounding box which this entity is tested against
   */
  public boolean insideBbox(BoundingBox otherBbox) {
    BoundingBox bbox = this.getBoundingBox();
    if (bbox == null) {
      return false;
    }
    return bbox.minLat >= otherBbox.minLat && bbox.maxLat <= otherBbox.maxLat
        && bbox.minLon >= otherBbox.minLon && bbox.maxLon <= otherBbox.maxLon;
  }

  /**
   * Returns the list of timestamps at which this entity was modified.
   *
   * If the parameter "recurse" is set to true, it will also include modifications of the object's
   * child elements (useful to find out when the geometry of this object has been altered).
   *
   * @param recurse specifies if times of modifications of child entities should also be returned or
   *        not
   * @return a list of timestamps where this entity has been modified
   */
  public abstract List<OSHDBTimestamp> getModificationTimestamps(boolean recurse);

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified.
   *
   * @return a list of timestamps where this entity has been modified
   */
  public List<OSHDBTimestamp> getModificationTimestamps() {
    return this.getModificationTimestamps(true);
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified and matches a given condition/filter.
   *
   * @param osmEntityFilter only timestamps for which the entity matches this filter are returned
   * @return a list of timestamps where this entity has been modified
   */
  public List<OSHDBTimestamp> getModificationTimestamps(Predicate<OSMEntity> osmEntityFilter) {
    if (this.getVersions().stream().noneMatch(osmEntityFilter)) {
      return new ArrayList<>();
    }

    List<OSHDBTimestamp> allModTs = this.getModificationTimestamps(true);
    List<OSHDBTimestamp> filteredModTs = new LinkedList<>();

    int timeIdx = allModTs.size() - 1;

    long lastOsmEntityTs = Long.MAX_VALUE;
    for (OSMEntity osmEntity : this) {
      OSHDBTimestamp osmEntityTs = osmEntity.getTimestamp();
      if (osmEntityTs.getRawUnixTimestamp() >= lastOsmEntityTs) {
        continue; // skip versions with identical (or invalid*) timestamps
      }
      OSHDBTimestamp modTs = allModTs.get(timeIdx);

      boolean matches = osmEntityFilter.test(osmEntity);

      if (matches) {
        while (modTs.getRawUnixTimestamp() >= osmEntityTs.getRawUnixTimestamp()) {
          filteredModTs.add(0, modTs);
          if (--timeIdx < 0) {
            break;
          }
          modTs = allModTs.get(timeIdx);
        }
      } else {
        while (timeIdx >= 0 && allModTs.get(timeIdx).getRawUnixTimestamp() > osmEntityTs.getRawUnixTimestamp()) {
          timeIdx--;
        }
      }
      lastOsmEntityTs = osmEntityTs.getRawUnixTimestamp();
    }
    return filteredModTs;
  }

  /**
   * Returns all timestamps at which this entity (or one or more of its child entities) has been
   * modified and matches a given condition/filter.
   *
   * If the groupedByChangeset parameter is set to true, consecutive modifications in a single
   * changeset are grouped together (only the last modification timestamp of the corresponding
   * changeset is returned). This can reduce the amount of geometry modifications by a lot (e.g.
   * when sequential node uploads of a way modification causes many intermediate modification
   * states), making results more "accurate"/comparable as well as faster processing of geometries.
   *
   * @param osmEntityFilter only timestamps for which the entity matches this filter are returned
   * @param groupedByChangeset if set, consecutive modifications of a single changeset are grouped
   *        together
   * @return a list of timestamps where this entity has been modified
   */
  public List<OSHDBTimestamp> getModificationTimestamps(Predicate<OSMEntity> osmEntityFilter,
      boolean groupedByChangeset) {
    List<OSHDBTimestamp> allModificationTimestamps = this.getModificationTimestamps(osmEntityFilter);
    if (!groupedByChangeset || allModificationTimestamps.size() <= 1) {
      return allModificationTimestamps;
    }

    // group modification timestamps by changeset
    List<OSHDBTimestamp> result = new ArrayList<>();
    Map<OSHDBTimestamp, Long> changesetTimestamps = this.getChangesetTimestamps();

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
   * Returns the changeset ids which correspond to modifications of this entity.
   *
   * Used internally to group modifications by changeset.
   *
   * @return a map between timestamps and changeset ids
   */
  protected abstract Map<OSHDBTimestamp, Long> getChangesetTimestamps();

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH, "ID:%d Vmax:+%d+ Creation:%d BBox:(%f,%f),(%f,%f)",
        id, getVersions().get(0).getVersion(),
        getVersions().get(getVersions().size() - 1).getTimestamp().getRawUnixTimestamp(),
        getBoundingBox().getMinLat(), getBoundingBox().getMinLon(),
        getBoundingBox().getMaxLat(), getBoundingBox().getMaxLon());
  }

}
