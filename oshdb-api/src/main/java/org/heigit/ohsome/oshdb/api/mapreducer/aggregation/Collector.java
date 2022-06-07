package org.heigit.ohsome.oshdb.api.mapreducer.aggregation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;

public class Collector {
  private Collector() {
    throw new IllegalStateException("Utility class");
  }

  public static <T> List<T> toList(MapReducer<T> mr) {
    return mr.reduce(Collector::init, Collector::combine, Collector::accumulate);
  }

  public static <U extends Comparable<U> & Serializable, T>
      SortedMap<U, List<T>> toList(MapAggregator<U, T> mr) {
    return mr.reduce(Collector::init, Collector::combine, Collector::accumulate);
  }

  private static <T> List<T> init() {
    return new LinkedList<>();
  }

  private static <T> List<T> combine(List<T> list, T e) {
    list.add(e);
    return list;
  }

  private static <T> List<T> accumulate(List<T> a, List<T> b) {
    var c = new ArrayList<T>(a.size() + b.size());
    c.addAll(a);
    c.addAll(b);
    return c;
  }
}
