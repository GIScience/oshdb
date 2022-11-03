package org.heigit.ohsome.oshdb.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.filter.GeometryFilter.ValueRange;
import org.heigit.ohsome.oshdb.filter.GeometryTypeFilter.GeometryType;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.jetbrains.annotations.Contract;
import org.jparsec.OperatorTable;
import org.jparsec.Parser;
import org.jparsec.Parsers;
import org.jparsec.Scanners;
import org.jparsec.Terminals.StringLiteral;
import org.jparsec.pattern.CharPredicates;
import org.jparsec.pattern.Patterns;

/**
 * A parser for OSM entity filters.
 *
 * <p>Such filters can select OSM entities by their tags, their type or other attributes. Filters
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
    this(tt, false);
  }

  /**
   * Creates a new parser for OSM entity filters.
   *
   * @param tt A tagtranslator object, used to transform OSM tags (e.g. "building=yes") to their
   *           respective OSHDB counterparts.
   * @param allowContributorFilters if true enables filtering by contributor/user id.
   */
  public FilterParser(TagTranslator tt, boolean allowContributorFilters) {
    // todo: refactor this method into smaller chunks to make it more easily testable
    final Parser<Void> whitespace = Scanners.WHITESPACES.skipMany();

    final Parser<String> keystr = Patterns.regex("[a-zA-Z_0-9:-]+")
        .toScanner("KEY_STRING (a-z, 0-9, : or -)")
        .source();
    final Parser<String> string = keystr.or(StringLiteral.DOUBLE_QUOTE_TOKENIZER);
    final Parser<Long> number = Scanners.INTEGER.map(Long::valueOf);
    final Parser<Double> stricterDecimal = Patterns.many1(CharPredicates.IS_DIGIT)
        .next(Patterns.isChar('.').next(Patterns.many1(CharPredicates.IS_DIGIT)).optional())
        .toScanner("decimal").source()
        .map(Double::valueOf);
    final Parser<Double> floatingNumber = Parsers.or(
        Scanners.SCIENTIFIC_NOTATION.map(Double::valueOf),
        stricterDecimal
    );

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
    final Parser<Void> area = Patterns.string("area").toScanner("area");
    final Parser<Void> length = Patterns.string("length").toScanner("length");
    final Parser<Void> perimeter = Patterns.string("perimeter").toScanner("perimeter");
    final Parser<Void> vertices = Patterns.string("geometry.vertices")
        .toScanner("geometry.vertices");
    final Parser<Void> outers = Patterns.string("geometry.outers").toScanner("geometry.outers");
    final Parser<Void> inners = Patterns.string("geometry.inners").toScanner("geometry.inners");
    final Parser<Void> roundness = Patterns.string("geometry.roundness")
        .toScanner("geometry.roundness");
    final Parser<Void> squareness = Patterns.string("geometry.squareness")
        .toScanner("geometry.squareness");
    final Parser<Void> changeset = Patterns.string("changeset").toScanner("changeset");
    final Parser<Void> contributor = Patterns.string("contributor").toScanner("contributor");

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
    final Parser<IdRange> range = Parsers.sequence(
        Scanners.isChar('('),
        whitespace,
        Parsers.or(
            Parsers.sequence(number, dotdot, number,
                (min, ignored, max) -> new IdRange(min, max)),
            number.followedBy(dotdot).map(
                min -> new IdRange(min, Long.MAX_VALUE)),
            Parsers.sequence(dotdot, number).map(
                max -> new IdRange(Long.MIN_VALUE, max))
        ).followedBy(whitespace).followedBy(Scanners.isChar(')')));
    final Parser<FilterExpression> rangeIdFilter = Parsers.sequence(
        id,
        colon,
        range)
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

    final Parser<ValueRange> positiveFloatingRange = Parsers.between(
        Scanners.isChar('('),
        Parsers.or(
            Parsers.sequence(floatingNumber, dotdot, floatingNumber,
                (min, ignored, max) -> new ValueRange(min, max)),
            floatingNumber.followedBy(dotdot).map(
                min -> new ValueRange(min, Double.POSITIVE_INFINITY)),
            Parsers.sequence(dotdot, floatingNumber).map(
                max -> new ValueRange(Double.NEGATIVE_INFINITY, max))
        ),
        Scanners.isChar(')')
    );
    final Parser<ValueRange> positiveIntegerRange = Parsers.between(
        Scanners.isChar('('),
        Parsers.or(
            Parsers.sequence(number, dotdot, number,
                (min, ignored, max) -> new ValueRange(min, max)),
            number.followedBy(dotdot).map(
                min -> new ValueRange(min, Double.POSITIVE_INFINITY)),
            Parsers.sequence(dotdot, number).map(
                max -> new ValueRange(0, max))
        ),
        Scanners.isChar(')')
    );

    // geometry filter
    final Parser<GeometryFilter> geometryFilterArea = Parsers.sequence(
        area, colon, positiveFloatingRange
    ).map(GeometryFilterArea::new);
    final Parser<GeometryFilter> geometryFilterLength = Parsers.sequence(
        length, colon, positiveFloatingRange
    ).map(GeometryFilterLength::new);
    final Parser<GeometryFilter> geometryFilterPerimeter = Parsers.sequence(
        perimeter, colon, positiveFloatingRange
    ).map(GeometryFilterPerimeter::new);
    final Parser<GeometryFilter> geometryFilterVertices = Parsers.sequence(
        vertices, colon, positiveIntegerRange
    ).map(GeometryFilterVertices::new);
    final Parser<GeometryFilter> geometryFilterOuters = Parsers.sequence(
        outers, colon, Parsers.or(positiveIntegerRange, number.map(n -> new ValueRange(n, n)))
    ).map(GeometryFilterOuterRings::new);
    final Parser<GeometryFilter> geometryFilterInners = Parsers.sequence(
        inners, colon, Parsers.or(positiveIntegerRange, number.map(n -> new ValueRange(n, n)))
    ).map(GeometryFilterInnerRings::new);
    final Parser<GeometryFilter> geometryFilterRoundness = Parsers.sequence(
        roundness, colon, positiveFloatingRange
    ).map(GeometryFilterRoundness::new);
    final Parser<GeometryFilter> geometryFilterSquareness = Parsers.sequence(
        squareness, colon, positiveFloatingRange
    ).map(GeometryFilterSquareness::new);
    final Parser<GeometryFilter> geometryFilter = Parsers.or(
        geometryFilterArea,
        geometryFilterLength,
        geometryFilterPerimeter,
        geometryFilterVertices,
        geometryFilterOuters,
        geometryFilterInners,
        geometryFilterRoundness,
        geometryFilterSquareness);

    // changeset id filters
    final Parser<ChangesetIdFilterEquals> changesetIdFilter = Parsers.sequence(
        changeset, colon, number
    ).map(ChangesetIdFilterEquals::new);
    final Parser<ChangesetIdFilterEqualsAnyOf> multiChangesetIdFilter = Parsers.sequence(
        changeset, colon, numberSequence
    ).map(ChangesetIdFilterEqualsAnyOf::new);
    final Parser<ChangesetIdFilterRange> rangeChangesetIdFilter = Parsers.sequence(
        changeset, colon, range
    ).map(ChangesetIdFilterRange::new);
    // contributor user id filters
    Parser<ContributorUserIdFilterEquals> contributorUserIdFilter = Parsers.sequence(
        contributor, colon, number
    ).map(ContributorUserIdFilterEquals::new);
    Parser<ContributorUserIdFilterEqualsAnyOf> multiContributorUserIdFilter = Parsers.sequence(
        contributor, colon, numberSequence
    ).map(ids -> ids.stream().map(Number::intValue).collect(Collectors.toList())
    ).map(ContributorUserIdFilterEqualsAnyOf::new);
    Parser<ContributorUserIdFilterRange> rangeContributorUserIdFilter = Parsers.sequence(
        contributor, colon, range
    ).map(ContributorUserIdFilterRange::new);
    if (!allowContributorFilters) {
      final var contributorFilterDisabled = Parsers.fail("contributor user id filter not enabled");
      contributorUserIdFilter = contributorUserIdFilter.followedBy(contributorFilterDisabled);
      multiContributorUserIdFilter =
          multiContributorUserIdFilter.followedBy(contributorFilterDisabled);
      rangeContributorUserIdFilter =
          rangeContributorUserIdFilter.followedBy(contributorFilterDisabled);
    }

    final Parser<FilterExpression> filter = Parsers.or(
        tagFilter,
        multiTagFilter,
        idFilter,
        idTypeFilter,
        multiIdFilter,
        multiIdTypeFilter,
        rangeIdFilter,
        typeFilter,
        geometryTypeFilter,
        geometryFilter,
        changesetIdFilter,
        multiChangesetIdFilter,
        rangeChangesetIdFilter,
        contributorUserIdFilter,
        multiContributorUserIdFilter,
        rangeContributorUserIdFilter);

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
