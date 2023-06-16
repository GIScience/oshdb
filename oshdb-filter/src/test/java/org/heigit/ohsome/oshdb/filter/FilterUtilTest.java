package org.heigit.ohsome.oshdb.filter;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterUtilTest extends FilterTest{

    @Test
    void extractIds() {
        extractIds("id:123", Set.of(123L));
        extractIds("id:123 or id:234", Set.of(123L, 234L));
        extractIds("id:123 and not id:234", Set.of(123L));
        extractIds("id:123 or name=heigit", Set.of());
        extractIds("id:(123, 234)", Set.of(123L, 234L));
        extractIds("id:(123 .. 125)", Set.of(123L, 124L, 125L));
    }

    private void extractIds(String filter, Set<Long> expected) {
       var expression = parser.parse(filter);
       var actual = FilterUtil.ids(expression);

       assertEquals(expected.size(), actual.size());
       assertTrue(expected.containsAll(actual));
    }

}