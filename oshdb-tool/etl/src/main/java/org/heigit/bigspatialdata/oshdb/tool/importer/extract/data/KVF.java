package org.heigit.bigspatialdata.oshdb.tool.importer.extract.data;

import java.util.Iterator;

public class KVF {

  public final String key;
  public final int freq;

  public final Iterator<VF> vfIterator;

  public KVF(String key, int freq, Iterator<VF> vfIterator) {
    this.key = key;
    this.freq = freq;
    this.vfIterator = vfIterator;
  }

}
