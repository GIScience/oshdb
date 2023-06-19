package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osm.OSMType;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.*;
import static java.util.Comparator.comparingLong;

public class FilterUtil {

    private FilterUtil() {
        throw new IllegalStateException("utility class");
    }

    public static Map<EnumSet<OSMType>, Set<OSHDBTag>> tags(FilterExpression expression, Predicate<OSHDBTag> test) {
        return tags(expression.normalize(), test);
    }

    public static Map<EnumSet<OSMType>, Set<OSHDBTag>> tags(List<List<Filter>> normalized, Predicate<OSHDBTag> test) {
        return extract(normalized, filter -> {
            if (filter instanceof TagFilterEquals tagFilter) {
                return Stream.of(tagFilter.getTag()).filter(test);
            } else if (filter instanceof TagFilterEqualsAny tagFilter) {
                return Stream.of(tagFilter.getTag()).map(key -> new OSHDBTag(key.toInt(), -1)).filter(test);
            } else if (filter instanceof TagFilterEqualsAnyOf tagFilter) {
                return tagFilter.tags.stream().filter(test);
            }
            return Stream.empty();
        });
    }

    private static <T> Map<EnumSet<OSMType>, Set<T>> extract(List<List<Filter>> normalized, Function<Filter, Stream<T>> extractor) {
        var result = new HashMap<EnumSet<OSMType>, Set<T>>();
        for(var group: normalized){
            var type = EnumSet.allOf(OSMType.class);
            var groupResult = new HashSet<T>();
            for (var filter : group) {
                if (filter instanceof TypeFilter typeFilter) {
                    type = EnumSet.of(typeFilter.getType());
                } else {
                    extractor.apply(filter).forEach(groupResult::add);
                }
            }
            if (groupResult.isEmpty()) {
                return emptyMap();
            }
            result.computeIfAbsent(type, x -> new HashSet<>()).addAll(groupResult);
        }
        return result;
    }

    public static Map<EnumSet<OSMType>, Set<IdRange>> extractIds(FilterExpression expression) {
        return extractIds(expression.normalize());
    }

    public static Map<EnumSet<OSMType>, Set<IdRange>> extractIds(List<List<Filter>> normalized) {
       var result =  extract(normalized, filter -> {
            if (filter instanceof IdFilterEquals idFilter){
                return Stream.of(new IdRange(idFilter.getId()));
            } else if (filter instanceof IdFilterEqualsAnyOf idFilter) {
                return idFilter.getIds().stream().map(IdRange::new);
            } else if (filter instanceof IdFilterRange idFilter) {
                return Stream.of(idFilter.getRange());
            }
            return Stream.empty();
        });
        compactRanges(result);
        return result;
    }

    private static void compactRanges(Map<EnumSet<OSMType>, Set<IdRange>> result) {
        for (var entry : result.entrySet()) {
            var ranges = new ArrayList<>(entry.getValue());
            var compact = new HashSet<IdRange>(ranges.size());
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
