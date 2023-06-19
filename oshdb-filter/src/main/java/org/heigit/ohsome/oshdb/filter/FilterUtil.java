package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.osm.OSMType;

import java.util.*;

import static java.util.Collections.*;
import static java.util.Comparator.comparingLong;

public class FilterUtil {

    private FilterUtil() {
        throw new IllegalStateException("utility class");
    }

    public static Map<EnumSet<OSMType>, List<IdRange>> ids(FilterExpression expression) {
        return ids(expression.normalize());
    }

    public static Map<EnumSet<OSMType>, List<IdRange>> ids(List<List<Filter>> normalized) {
        var result = new HashMap<EnumSet<OSMType>, List<IdRange>>();
        for (var group : normalized) {
            var type = EnumSet.allOf(OSMType.class);
            var groupRanges = new ArrayList<IdRange>();
            for (var filter: group) {
                if (filter instanceof TypeFilter typeFilter) {
                    type = EnumSet.of(typeFilter.getType());
                } else if (filter instanceof IdFilterEquals idFilter){
                    groupRanges.add(new IdRange(idFilter.getId()));
                } else if (filter instanceof IdFilterEqualsAnyOf idFilter) {
                    idFilter.getIds().stream().map(IdRange::new).forEach(groupRanges::add);
                } else if (filter instanceof IdFilterRange idFilter) {
                    groupRanges.add(idFilter.getRange());
                }
            }
            if (groupRanges.isEmpty()) {
                return emptyMap();
            }
            result.computeIfAbsent(type, x -> new ArrayList<>()).addAll(groupRanges);
        }
        compactRanges(result);
        return result;
    }

    private static void compactRanges(HashMap<EnumSet<OSMType>, List<IdRange>> result) {
        for (var entry : result.entrySet()) {
            var ranges = entry.getValue();
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
            entry.setValue(compact);
        }
    }
}
