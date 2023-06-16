package org.heigit.ohsome.oshdb.filter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptySet;

public class FilterUtil {

    private FilterUtil() {
        // utility class
    }

    public static Set<Long> ids(FilterExpression expression) {
        return ids(expression.normalize());
    }

    public static Set<Long> ids(List<List<Filter>> normalized) {
        var ids = new HashSet<Long>();
        for (var orGroups : normalized) {
            var groupIds = new HashSet<Long>();
            for (var filter: orGroups) {
                if (filter instanceof IdFilterEquals equals){
                    groupIds.add(equals.getId());
                } else if (filter instanceof IdFilterEqualsAnyOf equalsAnyOf) {
                    groupIds.addAll(equalsAnyOf.getIds());
                } else if (filter instanceof IdFilterRange range) {
                    range.getRange().getIds().forEach(groupIds::add);
                }
            }
            if (groupIds.isEmpty()) {
                return emptySet();
            }
            ids.addAll(groupIds);
        }
        return ids;
    }
}
