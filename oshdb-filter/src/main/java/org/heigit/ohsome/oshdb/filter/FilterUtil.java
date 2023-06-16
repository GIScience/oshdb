package org.heigit.ohsome.oshdb.filter;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Comparator.comparingLong;

public class FilterUtil {

    private FilterUtil() {
        // utility class
    }

    public static List<IdRange> ids(FilterExpression expression) {
        return ids(expression.normalize());
    }

    public static List<IdRange> ids(List<List<Filter>> normalized) {
        var ranges = new ArrayList<IdRange>();
        for (var orGroups : normalized) {
            var groupRanges = new ArrayList<IdRange>();
            for (var filter: orGroups) {
                if (filter instanceof IdFilterEquals equals){
                    groupRanges.add(new IdRange(equals.getId()));
                } else if (filter instanceof IdFilterEqualsAnyOf equalsAnyOf) {
                    equalsAnyOf.getIds().stream().map(IdRange::new).forEach(groupRanges::add);
                } else if (filter instanceof IdFilterRange range) {
                    groupRanges.add(range.getRange());
                }
            }
            if (groupRanges.isEmpty()) {
                return emptyList();
            }
            ranges.addAll(groupRanges);
        }
        var compact = new ArrayList<IdRange>(ranges.size());

        ranges.sort(comparingLong(IdRange::getFromId));
        var itr = ranges.iterator();
        var range = itr.next();
        while (itr.hasNext()) {
            var next = itr.next();
            if (next.getFromId() <= range.getToId() + 1) {
                range = new IdRange(range.getFromId(), next.getToId());
            } else {
                compact.add(range);
                range = next;
            }
        }
        compact.add(range);
        return compact;
    }
}
