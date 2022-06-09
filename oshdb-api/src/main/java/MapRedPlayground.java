import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapRedPlayground {

  private static class MR<X> {
    private Function<String, Stream<X>> transform;

    public MR(Function<String, Stream<X>> fnt) {
      this.transform = fnt;
    }

    public <R> MR<R> map(Function<X, R> map){
      var fnt = transform.andThen(x -> x.map(map));
      return new MR(fnt);
    }

    public <R> MR<R> map(BiFunction<String, X, R> map) {
      Function<String, Stream<R>> fnt = o -> transform.apply(o).map(x -> map.apply(o, x));
      return new MR<>(fnt);

    }

    public <R> MR<R> flatMap(Function<X, Stream<R>> map){
      var fnt = transform.andThen(x -> x.flatMap(map));
      return new MR<>(fnt);
    }

   public <R> MR<R> flatMap(BiFunction<String, Stream<X>, Stream<R>> map){
      return new MR<>(o -> map.apply(o, transform.apply(o)));
    }

  }

  public static void main(String[] args) {
      var mr = new MR<>(x -> Stream.of(x))
        .flatMap(s -> s.chars().boxed())
        //.flatMap((s, cs) -> Stream.of(Map.entry(s, cs.toArray(Integer[]::new))))
        .map((s,c) -> Map.entry(s,c))
        .flatMap((s, c) -> Stream.of(Map.entry(s, c.collect(Collectors.toList()))))


        ;

      Stream
        .of("123","abc", "a2!")
        .flatMap(s -> mr.transform.apply(s))
        .forEach(System.out::println);
  }

}
