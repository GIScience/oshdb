package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import java.util.Arrays;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.jparsec.OperatorTable;
import org.jparsec.Parser;
import org.jparsec.Parsers;
import org.jparsec.Scanners;
import org.jparsec.Terminals.StringLiteral;
import org.jparsec.pattern.Patterns;

public class FilterParser {
  private Parser<FilterExpression> parser;

  public FilterParser(TagTranslator tt) {
    final Parser<Void> whitespace = Scanners.WHITESPACES.skipMany();

    final Parser<String> keystr = Patterns.regex("[a-zA-Z][a-zA-Z_@0-9:]*").toScanner("KEY_STRING").source();
    final Parser<String> string = keystr.or(StringLiteral.DOUBLE_QUOTE_TOKENIZER);

    final Parser<String> equals = Patterns.string("=").toScanner("EQUALS").map(ignored -> "=");
    final Parser<String> notEquals = Patterns.string("!=").toScanner("NOT_EQUALS").map(ignored -> "!=");
    final Parser<String> colon = Patterns.string(":").toScanner("COLON").map(ignored -> ":");
    final Parser<String> type = Patterns.string("type").toScanner("TYPE").map(ignored -> "type");
    final Parser<String> node = Patterns.string("node").toScanner("NODE").map(ignored -> "node");
    final Parser<String> way = Patterns.string("way").toScanner("WAY").map(ignored -> "way");
    final Parser<String> relation = Patterns.string("relation").toScanner("RELATION").map(ignored -> "relation");
    final Parser<String> star = Patterns.string("*").toScanner("STAR").map(ignored -> "*");

    final Parser<FilterExpression> tagFilter = Parsers.list(Arrays.asList(
        string,
        whitespace,
        Parsers.or(equals, notEquals),
        whitespace,
        Parsers.or(string, star)
    )).map(l -> {
      if (l.get(4).equals("*")) {
        return new TagFilter(
            (String) l.get(2),
            tt.getOSHDBTagOf((String) l.get(0), (String) l.get(4))
        );
      } else {
        return new TagFilter(
            (String) l.get(2),
            tt.getOSHDBTagKeyOf((String) l.get(0))
        );
      }
    });
    final Parser<FilterExpression> typeFilter = Parsers.list(Arrays.asList(
        type,
        whitespace,
        colon,
        whitespace,
        Parsers.or(node, way, relation)
    )).map(l -> new TypeFilter((String) l.get(4)));

    final Parser<FilterExpression> filter = Parsers.or(tagFilter, typeFilter);

    final Parser<Void> parensStart = Patterns.string("(").toScanner("(");
    final Parser<Void> parensEnd = Patterns.string(")").toScanner(")");
    final Parser.Reference<FilterExpression> ref = Parser.newReference();
    final Parser<FilterExpression> unit = ref.lazy().between(parensStart, parensEnd)
        .or(filter);
    final Parser<String> and = whitespace
        .followedBy(Patterns.string("and").toScanner("AND"))
        .followedBy(whitespace)
        .map(ignored -> "or");
    final Parser<String> or = whitespace
        .followedBy(Patterns.string("or").toScanner("OR"))
        .followedBy(whitespace)
        .map(ignored -> "or");
    final Parser<String> not = whitespace
        .followedBy(Patterns.string("not").toScanner("NOT"))
        .followedBy(whitespace)
        .map(ignored -> "not");
    final Parser<FilterExpression> parser = new OperatorTable<FilterExpression>()
        .infixl(or.retn((a, b) -> new BinaryOperator(a, "or", b)), 10)
        .infixl(and.retn((a, b) -> new BinaryOperator(a, "and", b)), 20)
        .prefix(not.retn(x -> new UnaryOperator("not", x)), 50)
        .build(unit);
    ref.set(parser);
    this.parser = parser;
  }

  public FilterExpression parse(String str) {
    return this.parser.parse(str);
  }
}
