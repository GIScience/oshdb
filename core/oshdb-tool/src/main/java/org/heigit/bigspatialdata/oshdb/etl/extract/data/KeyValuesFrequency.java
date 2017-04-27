package org.heigit.bigspatialdata.oshdb.etl.extract.data;

import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;


public class KeyValuesFrequency implements Serializable, Comparable<KeyValuesFrequency> {
  private static final long serialVersionUID = 1L;
  private int freq = 0;
  private final SortedMap<String, Integer> values;
  
  public KeyValuesFrequency(){
	  values = new TreeMap<>();
  }
  
  public KeyValuesFrequency(int freq, final SortedMap<String,Integer> values){
	  this.freq = freq;
	  this.values = values;
  }

  public int compareTo(KeyValuesFrequency o) {
    return Integer.compare(freq, o.freq);
  }

  public void inc() {
    freq++;
  }

  public int freq() {
    return freq;
  }

  public SortedMap<String, Integer> values() {
    return values;
  }
}
