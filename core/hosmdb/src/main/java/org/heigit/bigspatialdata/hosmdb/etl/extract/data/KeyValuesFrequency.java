package org.heigit.bigspatialdata.hosmdb.etl.extract.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class KeyValuesFrequency implements Serializable, Comparable<KeyValuesFrequency> {
  private static final long serialVersionUID = 1L;
  private int freq = 0;
  private final Map<String, Integer> values = new HashMap<>();

  public int compareTo(KeyValuesFrequency o) {
    return Integer.compare(freq, o.freq);
  }

  public void inc() {
    freq++;
  }

  public int freq() {
    return freq;
  }

  public Map<String, Integer> values() {
    return values;
  }
}
