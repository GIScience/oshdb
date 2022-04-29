package org.heigit.ohsome.oshdb.util;

import java.util.function.Function;
import java.util.function.Supplier;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;

public class OSHDBUtil {
  private OSHDBUtil() {}

  public static <T> T switchOf(OSMType type, Supplier<T> node, Supplier<T> way,
      Supplier<T> relation) {
    switch (type) {
      case NODE:
        return node.get();
      case RELATION:
        return relation.get();
      case WAY:
        return way.get();
      default:
        throw new IllegalStateException();
    }
  }

  public static <T> T switchOf(OSHEntity osh, Function<OSHNode, T> node, Function<OSHWay, T> way,
      Function<OSHRelation, T> relation) {
    return switchOf(osh.getType(), () -> node.apply(cast(osh)), () -> way.apply(cast(osh)),
        () -> relation.apply(cast(osh)));
  }

  public static <T> T switchOf(OSMEntity osm, Function<OSMNode, T> node, Function<OSMWay, T> way,
      Function<OSMRelation, T> relation) {
    return switchOf(osm.getType(), () -> node.apply(cast(osm)), () -> way.apply(cast(osm)),
        () -> relation.apply(cast(osm)));
  }

  @SuppressWarnings("unchecked")
  public static <T extends OSHEntity> T cast(OSHEntity osh) {
    return (T) osh;
  }

  @SuppressWarnings("unchecked")
  public static <T extends OSMEntity> T cast(OSMEntity osm) {
    return (T) osm;
  }

}
