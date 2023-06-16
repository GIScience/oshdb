package org.heigit.ohsome.oshdb.filter;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FilterUtil {

    private FilterUtil() {
        // utility class
    }

    public static Set<Long> ids(FilterExpression expression) {
        var normalized = expression.normalize();
        return ids(normalized);
    }

    private static Set<Long> ids(List<List<Filter>> normalized) {
        var ids = new HashSet<Long>();
        for (var orGroups : normalized) {
            for (var filter: orGroups) {
                if (filter instanceof IdFilterEquals equals){
                    ids.add(equals.getId());
                } else if (filter instanceof IdFilterEqualsAnyOf equalsAnyOf) {
                    ids.addAll(equalsAnyOf.getIds());
                } else if (filter instanceof IdFilterRange range) {
                    range.getRange().getIds().forEach(ids::add);
                } else {
                    return Collections.emptySet();
                }
            }
        }
        return ids;
    }
}
