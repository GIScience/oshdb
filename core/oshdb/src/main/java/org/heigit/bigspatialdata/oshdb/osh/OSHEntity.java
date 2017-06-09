package org.heigit.bigspatialdata.oshdb.osh;


import java.io.IOException;
import java.io.ObjectOutput;
import java.util.*;
import java.util.function.Predicate;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.ByteArrayOutputWrapper;

@SuppressWarnings("rawtypes")
public abstract class OSHEntity<OSM extends OSMEntity> implements Comparable<OSHEntity>,Iterable<OSM> {
  
  public static final int NODE = 0;
  public static final int WAY = 1;
  public static final int RELATION = 2;

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

  public OSHEntity(final byte[] data, final int offset, final int length, 
      final long baseId,final long baseTimestamp, final long baseLongitude, final long baseLatitude,
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
    if (offset == 0 && length == data.length)
      return data;
    byte[] result = new byte[length];
    System.arraycopy(data, offset, result, 0, length);
    return result;
  }
  
  public long getId() {
    return id;
  }

  public abstract int getType();

  public int getLength() {
    return length;
  }
  
  public BoundingBox getBoundingBox(){
    return bbox;
  }
  
  public int[] getKeys(){
    return keys;
  }
  
  public abstract List<OSM> getVersions();

  /* byTimestamps is assumed to be presorted, otherwise output is undetermined */
  public SortedMap<Long,OSM> getByTimestamps(List<Long> byTimestamps){
    SortedMap<Long,OSM> result = new TreeMap<>();
	  
	  int i = byTimestamps.size()-1;
	  Iterator<OSM> itr = iterator();
	  while(itr.hasNext() && i >= 0){
		  OSM osm = itr.next();
		  if(osm.getTimestamp() > byTimestamps.get(i)){
			  continue;
		  } else {
			  while (i >= 0 && osm.getTimestamp() <= byTimestamps.get(i)) {
				  result.put(byTimestamps.get(i), osm);
				  i--;
			  }
		  }
	  }
	  return result;
  }

  public Map<Long,OSM> getByTimestamps(){ // todo: name of method?
	  Map<Long,OSM> result = new TreeMap<>();
	  Iterator<OSM> itr = iterator();
	  while(itr.hasNext()){
		  OSM osm = itr.next();
		  result.put(osm.getTimestamp(), osm);
	  }
	  return result;
	  // todo: replace with call to getBetweenTimestamps(-Infinity, Infinity):
  	  //return this.getBetweenTimestamps(Long.MIN_VALUE, Long.MAX_VALUE);
  }

  public OSM getByTimestamp(long timestamp){
	  Iterator<OSM> itr = iterator();
	  while(itr.hasNext()){
		  OSM osm = itr.next();
		  if (osm.getTimestamp() <= timestamp)
		  	  return osm;
	  }
	  return null;
  }
  
  public List<OSM> getBetweenTimestamps(final long t1, final long t2){
	  final long maxTimestamp = Math.max(t1, t2);
	  final long minTimestamp = Math.min(t1, t2);
	  
	  List<OSM> result = new ArrayList<>();
	  
	  Iterator<OSM> itr = iterator();
	  while(itr.hasNext()){
		  OSM osm = itr.next();
		  if(osm.getTimestamp() > maxTimestamp)
			  continue;
		  result.add(osm);
		  if(osm.getTimestamp() < minTimestamp)
			  break;
	  }  
	  return result;
  }
  
  public boolean hasTagKey(int key){
  	// todo: replace with binary search (keys are sorted)
    for(int i=0; i<keys.length; i++){
		if(keys[i] == key)
			return true;
      if(keys[i] > key)
    	  break;
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

  /*
   * returns true if the bbox of this HOSM entity intersects (or is fully inside) the given bbox.
   * Used to roughly pre-filter objects against a bbox.
   * See https://gitlab.com/giscience/osh-bigdb/OSH-BigDB/issues/11
   */
  public boolean intersectsBbox(BoundingBox otherBbox) {
    BoundingBox bbox = this.getBoundingBox();
    if (bbox == null) return false;
  	if (this.insideBbox(otherBbox))
      return true;
    if (bbox.minLat >= otherBbox.minLat && bbox.minLat <= otherBbox.maxLat && (
      bbox.minLon >= otherBbox.minLon && bbox.minLon <= otherBbox.maxLon ||
      bbox.maxLon >= otherBbox.minLon && bbox.maxLon <= otherBbox.maxLon))
      return true;
    if (bbox.maxLat >= otherBbox.minLat && bbox.maxLat <= otherBbox.maxLat && (
      bbox.minLon >= otherBbox.minLon && bbox.minLon <= otherBbox.maxLon ||
      bbox.maxLon >= otherBbox.minLon && bbox.maxLon <= otherBbox.maxLon))
      return true;
    return false;
  }

  /*
   * returns true if the bbox of this HOSM entity is fully inside the given bbox.
   * Can be used as an optimization to find not-to-be-clipped entity Geometries
   * (see https://gitlab.com/giscience/osh-bigdb/OSH-BigDB/issues/13)
   * todo: extend funtionality for non-bbox case: insidePolygon(poly)
   */
  public boolean insideBbox(BoundingBox otherBbox) {
    BoundingBox bbox = this.getBoundingBox();
    if (bbox == null) return false;
  	return
        bbox.minLat >= otherBbox.minLat && bbox.maxLat <= otherBbox.maxLat &&
        bbox.minLon >= otherBbox.minLon && bbox.maxLon <= otherBbox.maxLon;
  }

  /*
   * returns the list of timestamps at which this entity was modified.
   * If the parameter "recurse" is set to true, it will also include
   * modifications of the object's child elements (useful to find out
   * when the geometry of this object has been altered).
   */
  public abstract List<Long> getModificationTimestamps(boolean recurse);

  public List<Long> getModificationTimestamps() {
      return this.getModificationTimestamps(true);
  }

  /*
   * returns only the modification timestamps of an object where it
   * matches a given condition/filter
   */
  public List<Long> getModificationTimestamps(Predicate<OSMEntity> osmEntityFilter) {
    if (!this.getVersions().stream().anyMatch(osmEntityFilter))
      return new ArrayList<>();

    List<Long> allModTs = this.getModificationTimestamps(true);
    List<Long> filteredModTs = new LinkedList<>();

    int timeIdx = allModTs.size()-1;

    long lastOsmEntityTs = Long.MAX_VALUE;
    for (OSMEntity osmEntity : this) {
      long osmEntityTs = osmEntity.getTimestamp();
      if (osmEntityTs >= lastOsmEntityTs) continue; // skip versions with identical (or invalid*) timestamps
      long modTs = allModTs.get(timeIdx);

      boolean matches = osmEntityFilter.test(osmEntity);

      if (matches) {
        while (modTs >= osmEntityTs) {
          filteredModTs.add(0, modTs);
          if (--timeIdx < 0) break;
          modTs = allModTs.get(timeIdx);
        }
      } else {
        while (timeIdx >= 0 && allModTs.get(timeIdx) > osmEntityTs) {
          timeIdx--;
        }
      }
      lastOsmEntityTs = osmEntityTs;
    }
    return filteredModTs;
  };
}
