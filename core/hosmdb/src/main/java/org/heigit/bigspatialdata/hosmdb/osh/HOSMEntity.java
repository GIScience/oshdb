package org.heigit.bigspatialdata.hosmdb.osh;


import java.io.IOException;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.heigit.bigspatialdata.hosmdb.osm.OSMEntity;
import org.heigit.bigspatialdata.hosmdb.util.BoundingBox;
import org.heigit.bigspatialdata.hosmdb.util.ByteArrayOutputWrapper;

public abstract class HOSMEntity<OSM extends OSMEntity> implements Comparable<HOSMEntity>,Iterable<OSM> {
  
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

  public HOSMEntity(final byte[] data, final int offset, final int length, 
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
  public Map<Long,OSM> getByTimestamps(List<Long> byTimestamps){
	  Map<Long,OSM> result = new TreeMap<>();
	  
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
  
  public boolean hasKey(int key){
  	// todo: replace with binary search (keys are sorted)
    for(int i=0; i<keys.length; i++){
		if(keys[i] == key)
			return true;
      if(keys[i] > key)
    	  break;
    }
    return false;
  }
  
  public abstract HOSMEntity<OSM> rebase(long baseId, long baseTimestamp, long baseLongitude,
	      long baseLatitude) throws IOException;

  @Override
  public int compareTo(HOSMEntity o) {
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
  public boolean intersectsBbox(BoundingBox bbox) {
  	if (this.insideBbox(bbox))
      return true;
    if (this.bbox.minLat >= bbox.minLat && this.bbox.minLat <= bbox.maxLat && (
      this.bbox.minLon >= bbox.minLon && this.bbox.minLon <= bbox.maxLon ||
      this.bbox.maxLon >= bbox.minLon && this.bbox.maxLon <= bbox.maxLon))
      return true;
    if (this.bbox.maxLat >= bbox.minLat && this.bbox.maxLat <= bbox.maxLat && (
      this.bbox.minLon >= bbox.minLon && this.bbox.minLon <= bbox.maxLon ||
      this.bbox.maxLon >= bbox.minLon && this.bbox.maxLon <= bbox.maxLon))
      return true;
    return false;
  }

  /*
   * returns true if the bbox of this HOSM entity is fully inside the given bbox.
   * Can be used as an optimization to find not-to-be-clipped entity Geometries
   * (see https://gitlab.com/giscience/osh-bigdb/OSH-BigDB/issues/13)
   * todo: extend funtionality for non-bbox case: insidePolygon(poly)
   */
  public boolean insideBbox(BoundingBox bbox) {
  	return
      this.bbox.minLat >= bbox.minLat && this.bbox.maxLat <= bbox.maxLat &&
      this.bbox.minLon >= bbox.minLon && this.bbox.maxLon <= bbox.maxLon;
  }

}
