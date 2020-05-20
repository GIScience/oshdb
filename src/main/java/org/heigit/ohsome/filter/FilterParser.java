package org.heigit.ohsome.filter;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
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

    final Parser<TagFilter.Type> equals = Patterns.string("=")
        .toScanner("EQUALS (=)")
        .map(ignored -> TagFilter.Type.EQUALS);
    final Parser<TagFilter.Type> notEquals = Patterns.string("!=")
        .toScanner("NOT_EQUALS (!=)")
        .map(ignored -> TagFilter.Type.NOT_EQUALS);
    final Parser<Void> colon = Patterns.string(":").toScanner("COLON (:)");
    final Parser<Void> type = Patterns.string("type").toScanner("type");
    final Parser<OSMType> node = Patterns.string("node").toScanner("node")
        .map(ignored -> OSMType.NODE);
    final Parser<OSMType> way = Patterns.string("way").toScanner("way")
        .map(ignored -> OSMType.WAY);
    final Parser<OSMType> relation = Patterns.string("relation").toScanner("relation")
        .map(ignored -> OSMType.RELATION);
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
    final Parser<FilterExpression> typeFilter = Parsers.sequence(
        type,
        whitespace,
        colon,
        whitespace,
        Parsers.or(node, way, relation))
        .map(TypeFilter::new);
    final Parser<FilterExpression> geometryTypeFilter = Parsers.sequence(
        geometry,
        whitespace,
        colon,
        whitespace,
        Parsers.or(point, line, polygon, other))
        .map(geometryType -> new GeometryTypeFilter(geometryType, tt));

    final Parser<FilterExpression> filter = Parsers.or(tagFilter, typeFilter, geometryTypeFilter);

    final Parser<Void> parensStart = Patterns.string("(").toScanner("(");
    final Parser<Void> parensEnd = Patterns.string(")").toScanner(")");
    final Parser.Reference<FilterExpression> ref = Parser.newReference();
    final Parser<FilterExpression> unit = ref.lazy().between(parensStart, parensEnd)
        .or(filter);
    final Parser<Void> and = whitespace
        .followedBy(Patterns.string("and").toScanner("and"))
        .followedBy(whitespace);
    final Parser<Void> or = whitespace
        .followedBy(Patterns.string("or").toScanner("or"))
        .followedBy(whitespace);
    final Parser<Void> not = whitespace
        .followedBy(Patterns.string("not").toScanner("not"))
        .followedBy(whitespace);
    final Parser<FilterExpression> parser = new OperatorTable<FilterExpression>()
        .infixl(or.retn((a, b) -> BinaryOperator.fromOperator(a, BinaryOperator.Type.OR, b)), 10)
        .infixl(and.retn((a, b) -> BinaryOperator.fromOperator(a, BinaryOperator.Type.AND, b)), 20)
        .prefix(not.retn(FilterExpression::negate), 50)
        .build(unit);
    ref.set(parser);
    this.parser = parser;
  }

  /**
   * Parse a filter expression.
   *
   * @param str A string representing an OSM entity filter.
   * @return A tree structure representing this filter, can be applied to OSM entities.
   */
  @Contract(pure = true)
  public FilterExpression parse(String str) {
    return this.parser.parse(str);
  }
}
