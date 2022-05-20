package org.heigit.ohsome.oshdb.contribution;

import static org.heigit.ohsome.oshdb.OSHDBTest.tag;
import static org.heigit.ohsome.oshdb.OSHDBTest.tags;

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
      Arbitrary<int[]> tagArrays = Arbitraries.just(tags(tag(1, 1))); // Randomize
      Arbitrary<Tuple4<Integer, Integer, Integer, int[]>> versions = Combinators.combine(
              timestampDiffs, changesets, tagArrays)
          .as((diff, changeset, tags) -> Tuple.of(diff, changeset.get1(), changeset.get2(), tags));
      return versions.list().ofMinSize(1).ofMaxSize(100)
          .map(tuples -> {
            List<OSMNode> versionList = new ArrayList<>();
            long timestamp = 0;
            int version = 1;
            for (Tuple4<Integer, Integer, Integer, int[]> tuple : tuples) {
              timestamp += tuple.get1();
              OSMNode node = OSM.node(id, version, timestamp, tuple.get2(), tuple.get3(),
                  tuple.get4(), 1, 0);
              versionList.add(node);
              version++;
            }
            return versionList;
          });
    });
  }
}
