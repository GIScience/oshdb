package org.heigit.ohsome.filter;

import java.util.ArrayList;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.ohsome.filter.GeometryTypeFilter.GeometryType;
import org.jetbrains.annotations.Contract;
import org.jparsec.OperatorTable;
import org.jparsec.Parser;
import org.jparsec.Parsers;
import org.jparsec.Scanners;
import org.jparsec.Terminals.StringLiteral;
import org.jparsec.pattern.Patterns;

/**
 * A parser for OSM entity filters.
 *
 * <p>Such filters can select OSM entites by their tags, their type or other attributes. Filters
 * can contain boolean operators (and/or/not) and parentheses can be used.</p>
 *
 * <p>Example: "type:way and highway=residential and not (lit=yes or lit=automatic)"</p>
 */
public class FilterParser {
  private final Parser<FilterExpression> parser;

  /**
   * Creates a new parser for OSM entity filters.
   *
   * @param tt A tagtranslator object, used to transform OSM tags (e.g. "building=yes") to their
   *           respective OSHDB counterparts.
   */
  public FilterParser(TagTranslator tt) {
    final Parser<Void> whitespace = Scanners.WHITESPACES.skipMany();

    final Parser<String> keystr = Patterns.regex("[a-zA-Z_0-9:-]+")
        .toScanner("KEY_STRING (a-z, 0-9, : or -)")
        .source();
    final Parser<String> string = keystr.or(StringLiteral.DOUBLE_QUOTE_TOKENIZER);
    final Parser<Long> number = Scanners.INTEGER.map(Long::valueOf);

    final Parser<TagFilter.Type> equals = Patterns.string("=")
        .toScanner("EQUALS (=)")
        .map(ignored -> TagFilter.Type.EQUALS);
    final Parser<TagFilter.Type> notEquals = Patterns.string("!=")
        .toScanner("NOT_EQUALS (!=)")
        .map(ignored -> TagFilter.Type.NOT_EQUALS);
    final Parser<Void> colon = whitespace.followedBy(
        Patterns.string(":").toScanner("COLON (:)")).followedBy(whitespace);
    final Parser<Void> slash = Patterns.string("/").toScanner("SLASH (/)");
    final Parser<Void> id = Patterns.string("id").toScanner("id");
    final Parser<Void> type = Patterns.string("type").toScanner("type");
    final Parser<OSMType> node = Patterns.string("node").toScanner("node")
        .map(ignored -> OSMType.NODE);
    final Parser<OSMType> way = Patterns.string("way").toScanner("way")
        .map(ignored -> OSMType.WAY);
    final Parser<OSMType> relation = Patterns.string("relation").toScanner("relation")
        .map(ignored -> OSMType.RELATION);
    final Parser<OSMType> osmTypes = Parsers.or(node, way, relation);
    final Parser<Void> geometry = Patterns.string("geometry").toScanner("geometry");
    final Parser<GeometryType> point = Patterns.string("point").toScanner("point")
        .map(ignored -> GeometryType.POINT);
    final Parser<GeometryType> line = Patterns.string("line").toScanner("line")
        .map(ignored -> GeometryType.LINE);
    final Parser<GeometryType> polygon = Patterns.string("polygon").toScanner("polygon")
        .map(ignored -> GeometryType.POLYGON);
    final Parser<GeometryType> other = Patterns.string("other").toScanner("other")
        .map(ignored -> GeometryType.OTHER);
    final Parser<String> star = Patterns.string("*").toScanner("STAR (*)")
        .map(ignored -> "*");

    final Parser<FilterExpression> tagFilter = Parsers.sequence(
        string,
        whitespace,
        Parsers.or(equals, notEquals),
        whitespace,
        Parsers.or(string, star),
        (key, ignored, selector, ignored2, value) -> {
          OSMTagInterface tag = value.equals("*")
              ? new OSMTagKey(key)
              : new OSMTag(key, value);
          return TagFilter.fromSelector(selector, tag, tt);
        });
    final Parser<Void> in = whitespace
        .followedBy(Patterns.string("in").toScanner("in"))
        .followedBy(whitespace);
    final Parser<List<String>> stringSequence = Parsers.sequence(
        Scanners.isChar('('),
        string.sepBy(whitespace.followedBy(Scanners.isChar(',')).followedBy(whitespace)),
        Scanners.isChar(')'),
        (ignored, list, ignored2) -> list);
    final Parser<FilterExpression> multiTagFilter = Parsers.sequence(
        string,
        in,
        stringSequence,
        (key, ignored, values) -> {
          List<OSHDBTag> tags = new ArrayList<>(values.size());
          values.forEach(value -> tags.add(tt.getOSHDBTagOf(key, value)));
          return new TagFilterEqualsAnyOf(tags);
        });
    final Parser<FilterExpression> idFilter = Parsers.sequence(
        id,
        colon,
        number)
        .map(IdFilterEquals::new);
    final Parser<FilterExpression> osmTypeIdParser = Parsers.sequence(
        osmTypes.followedBy(slash),
        number,
        (osmType, osmId) -> new AndOperator(new TypeFilter(osmType), new IdFilterEquals(osmId)));
    final Parser<FilterExpression> idTypeFilter = Parsers.sequence(
        id,
        colon,
        osmTypeIdParser);
    final Parser<Void> comma = whitespace.followedBy(Scanners.isChar(',')).followedBy(whitespace);
    final Parser<List<Long>> numberSequence = Parsers.sequence(
        Scanners.isChar('('),
        number.sepBy(comma),
        Scanners.isChar(')'),
        (ignored, list, ignored2) -> list);
    final Parser<FilterExpression> multiIdFilter = Parsers.sequence(
        id,
        colon,
        numberSequence)
        .map(IdFilterEqualsAnyOf::new);
    final Parser<List<FilterExpression>> idTypeSequence = Parsers.sequence(
        Scanners.isChar('('),
        osmTypeIdParser.sepBy(comma),
        Scanners.isChar(')'),
        (ignored, list, ignored2) -> list);
    final Parser<FilterExpression> multiIdTypeFilter = Parsers.sequence(
        id,
        colon,
        idTypeSequence)
        .map(list -> list.stream()
            .reduce(OrOperator::new)
            .orElse(new ConstantFilter(false)));
    final Parser<Void> dotdot = whitespace
        .followedBy(Patterns.string("..").toScanner("DOT-DOT (..)"))
        .followedBy(whitespace);
    final Parser<FilterExpression> rangeIdFilter = Parsers.sequence(
        id,
        colon,
        Scanners.isChar('('),
        whitespace,
        Parsers.or(
            Parsers.sequence(number, dotdot, number,
                (min, ignored, max) -> new IdFilterRange.IdRange(min, max)),
            Parsers.sequence(number, dotdot,
                (min, ignored2) -> new IdFilterRange.IdRange(min, Long.MAX_VALUE)),
            Parsers.sequence(dotdot, number,
                (ignored, max) -> new IdFilterRange.IdRange(Long.MIN_VALUE, max))
        ).followedBy(whitespace).followedBy(Scanners.isChar(')')))
        .map(IdFilterRange::new);
    final Parser<FilterExpression> typeFilter = Parsers.sequence(
        type,
        colon,
        osmTypes)
        .map(TypeFilter::new);
    final Parser<FilterExpression> geometryTypeFilter = Parsers.sequence(
        geometry,
        colon,
        Parsers.or(point, line, polygon, other))
        .map(geometryType -> new GeometryTypeFilter(geometryType, tt));

    final Parser<FilterExpression> filter = Parsers.or(
        tagFilter,
        multiTagFilter,
        idFilter,
        idTypeFilter,
        multiIdFilter,
        multiIdTypeFilter,
        rangeIdFilter,
        typeFilter,
        geometryTypeFilter);

    final Parser<Void> parensStart = Patterns.string("(").toScanner("(").followedBy(whitespace);
    final Parser<Void> parensEnd = whitespace.followedBy(Patterns.string(")").toScanner(")"));
    final Parser.Reference<FilterExpression> ref = Parser.newReference();
    final Parser<FilterExpression> unit = ref.lazy().between(parensStart, parensEnd)
        .or(filter);
    final Parser<Void> and = whitespace
        .followedBy(Patterns.string("and").toScanner("and"))
        .followedBy(whitespace);
    final Parser<Void> or = whitespace
        .followedBy(Patterns.string("or").toScanner("or"))
        .followedBy(whitespace);
    final Parser<Void> not = Patterns.string("not").toScanner("not")
        .followedBy(whitespace);
    final Parser<ConstantFilter> emptyFilter = Patterns.EOF.toScanner("EOF")
        .map(ignored -> new ConstantFilter(true));
    final Parser<FilterExpression> parser = new OperatorTable<FilterExpression>()
        .infixl(or.retn((a, b) -> BinaryOperator.fromOperator(a, BinaryOperator.Type.OR, b)), 10)
        .infixl(and.retn((a, b) -> BinaryOperator.fromOperator(a, BinaryOperator.Type.AND, b)), 20)
        .prefix(not.retn(FilterExpression::negate), 50)
        .build(unit);
    ref.set(parser);
    this.parser = Parsers.or(emptyFilter, parser);
  }

  /**
   * Parse a filter expression.
   *
   * @param str A string representing an OSM entity filter.
   * @return A tree structure representing this filter, can be applied to OSM entities.
   */
  @Contract(pure = true)
  public FilterExpression parse(String str) {
    return this.parser.parse(str.strip());
  }
}
