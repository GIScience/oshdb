package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterUtilTest extends FilterTest{

    @Test
    void extractIds() {
        extractIds("id:123", Map.of(EnumSet.allOf(OSMType.class),List.of(new IdRange(123L))));
        extractIds("id:123 or id:234", Map.of(EnumSet.allOf(OSMType.class),List.of(new IdRange(123L), new IdRange(234L))));
        extractIds("id:123 or id:124", Map.of(EnumSet.allOf(OSMType.class),List.of(new IdRange(123L, 124L))));
        extractIds("id:123 and not id:234", Map.of(EnumSet.allOf(OSMType.class),List.of(new IdRange(123L))));
        extractIds("id:123 or name=heigit", Map.of());
        extractIds("id:(123, 234)", Map.of(EnumSet.allOf(OSMType.class),List.of(new IdRange(123L), new IdRange(234L))));
        extractIds("id:(123 .. 125)", Map.of(EnumSet.allOf(OSMType.class),List.of(new IdRange(123L, 125L))));
    }

    @Test
    void extractIdsWithTypes() {
        extractIds("id:node/123 or id:way/234", Map.of(
                EnumSet.of(OSMType.NODE), List.of(new IdRange(123L)),
                EnumSet.of(OSMType.WAY), List.of(new IdRange(234L))));
        extractIds("id:node/123 or id:way/234 or id:way/235", Map.of(
                EnumSet.of(OSMType.NODE), List.of(new IdRange(123L)),
                EnumSet.of(OSMType.WAY), List.of(new IdRange(234L, 235L))));
        extractIds("id:node/123 or (type:way and id:234)", Map.of(
                EnumSet.of(OSMType.NODE), List.of(new IdRange(123L)),
                EnumSet.of(OSMType.WAY), List.of(new IdRange(234L))));
        extractIds("id:node/123 or id:234", Map.of(
                EnumSet.of(OSMType.NODE), List.of(new IdRange(123L)),
                EnumSet.allOf(OSMType.class), List.of(new IdRange(234L))));
        extractIds("not type:node and id:123", Map.of(
                EnumSet.of(OSMType.WAY), List.of(new IdRange(123L)),
                EnumSet.of(OSMType.RELATION), List.of(new IdRange(123L))));
    }

    @Test
    void extractTags() {
        var expression = parser.parse("type:way and (highway=primary or building=*)");
        var result = FilterUtil.tags(expression, x -> true);
        assertTrue(result.containsKey(EnumSet.of(OSMType.WAY)));
        assertTrue(result.get(EnumSet.of(OSMType.WAY)).containsAll(List.of(new OSHDBTag(2,7), new OSHDBTag(1,-1))));
    }

    private void extractIds(String filter, Map<EnumSet<OSMType>, List<IdRange>> expected) {
       var expression = parser.parse(filter);
       var actual = FilterUtil.extractIds(expression);

       assertEquals(expected.size(), actual.size());
       for (var entry : expected.entrySet()) {
           assertTrue(actual.containsKey(entry.getKey()));
           assertEquals(entry.getValue().size(), actual.get(entry.getKey()).size());
           assertTrue(actual.get(entry.getKey()).containsAll(entry.getValue()));
       }
    }

}