package org.heigit.ohsome.oshdb.api.mapreducer.reduction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorBase;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;

public abstract class Collector<X, S> {
  private static class ListCollector<X> extends Collector<X, List<X>> {

    @Override
    public List<X> apply(MapReducerBase<X> mr) {
      return mr.<List<X>>reduce(Collector::init, Collector::combine, Collector::accumulate);
    }

  }

  public abstract S apply(MapReducerBase<X> mapReducerBase);

  public static <X> Collector<X, List<X>> toList(){
    return new ListCollector<>();
  }

  public static <T> List<T> toList(MapReducerBase<T> mr) {
    return mr.<List<T>>reduce(Collector::init, Collector::combine, Collector::accumulate);
  }

  public static <U, T>
  Map<U, List<T>> toList(MapAggregatorBase<U, T> mr) {
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
