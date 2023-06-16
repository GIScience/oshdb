package org.heigit.ohsome.oshdb.filter;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterUtilTest extends FilterTest{

    @Test
    void extractIds() {
        extractIds("id:123", List.of(new IdRange(123L)));
        extractIds("id:123 or id:234", List.of(new IdRange(123L), new IdRange(234L)));
        extractIds("id:123 or id:124", List.of(new IdRange(123L, 124L)));
        extractIds("id:123 and not id:234", List.of(new IdRange(123L)));
        extractIds("id:123 or name=heigit", List.of());
        extractIds("id:(123, 234)", List.of(new IdRange(123L), new IdRange(234L)));
        extractIds("id:(123 .. 125)", List.of(new IdRange(123L, 125L)));
    }

    private void extractIds(String filter, List<IdRange> expected) {
       var expression = parser.parse(filter);
       var actual = FilterUtil.ids(expression);

       assertEquals(expected.size(), actual.size());
       assertTrue(expected.containsAll(actual));
    }

}