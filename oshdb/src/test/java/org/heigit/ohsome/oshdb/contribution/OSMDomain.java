package org.heigit.ohsome.oshdb.contribution;

import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;
import net.jqwik.api.Tuple.Tuple2;
import net.jqwik.api.Tuple.Tuple4;
import net.jqwik.api.domains.DomainContextBase;
import net.jqwik.api.providers.TypeUsage;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMNode;

public class OSMDomain extends DomainContextBase {

  @Provide
  Arbitrary<List<OSMNode>> versions(TypeUsage targetType) {
    Arbitrary<Integer> ids = Arbitraries.integers().between(1, 1_000_000_000);
    return ids.flatMap(id -> {
      Arbitrary<Integer> timestampDiffs = Arbitraries.integers().between(0, 100_000);
      Arbitrary<Tuple2<Integer, Integer>> changesets = Arbitraries.of(
          Tuple.of(101, 1),
          Tuple.of(202, 2),
          Tuple.of(303, 3)
      );
      Arbitrary<int[]> tagArrays = tags();
      Arbitrary<Tuple2<Integer, Integer>> coordinates = coordinates();
      Arbitrary<Tuple4<Integer, Tuple2<Integer, Integer>, int[], Tuple2<Integer, Integer>>> versions = Combinators.combine(
          timestampDiffs, changesets, tagArrays, coordinates
      ).as(Tuple::of);
      return versions.list().ofMinSize(1).ofMaxSize(100)
          .map(tuples -> {
            List<OSMNode> versionList = new ArrayList<>();
            long timestamp = 0;
            int version = 1;
            for (Tuple4<Integer, Tuple2<Integer, Integer>, int[], Tuple2<Integer, Integer>> tuple : tuples) {
              timestamp += tuple.get1();
              Tuple2<Integer, Integer> changeset = tuple.get2();
              Tuple2<Integer, Integer> coords = tuple.get4();
              OSMNode node = OSM.node(
                  id, version, timestamp, changeset.get1(), changeset.get2(),
                  tuple.get3(), coords.get1(), coords.get2()
              );
              versionList.add(node);
              version++;
            }
            return versionList;
          });
    });
  }

  private Arbitrary<Tuple2<Integer, Integer>> coordinates() {
    Arbitrary<Integer> longitudes = Arbitraries.integers().between(-10000, 10000); // Real borders?
    Arbitrary<Integer> latitudes = Arbitraries.integers().between(-10000, 10000); // Real borders?
    return Combinators.combine(longitudes, latitudes).as(Tuple::of);
  }

  private Arbitrary<int[]> tags() {
    Arbitrary<Integer> keys = Arbitraries.integers().between(1, 1000);
    Arbitrary<Integer> values = Arbitraries.integers().between(1, 1000);
    return Combinators.combine(keys, values).as(Tuple::of)
        .list().ofMaxSize(10)
        .map(listOfTags -> {
          int[] tags = new int[listOfTags.size() * 2];
          for (int i = 0; i < listOfTags.size(); i++) {
            tags[2 * i + 0] = listOfTags.get(i).get1();
            tags[2 * i + 1] = listOfTags.get(i).get2();
          }
          return tags;
        });
  }
}
