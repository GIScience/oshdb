package org.heigit.ohsome.oshdb.tool.importer.extract.data;

import java.util.Iterator;

public class KeyValueFrequency {

  public final String key;
  public final int freq;

  public final Iterator<ValueFrequency> vfIterator;

  public KeyValueFrequency(String key, int freq, Iterator<ValueFrequency> vfIterator) {
    this.key = key;
    this.freq = freq;
    this.vfIterator = vfIterator;
  }

}
