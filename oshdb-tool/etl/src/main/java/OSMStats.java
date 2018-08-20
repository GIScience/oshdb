import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.reactive.MyLambdaSubscriber;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Node;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Relation;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.TagText;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Way;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;
import org.heigit.bigspatialdata.oshpbf.parser.rx.RxOshPbfReader;
import org.reactivestreams.Publisher;
import com.google.common.base.Stopwatch;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;

public class OSMStats {

  private final Path pbf;


  private long globalNumberOfTags = 0;
  private long globalNumberOfEntrys = 0;
  private long globalNumberOfVersions = 0;
  private List<Integer> numberOfTagsForHistogramm = new ArrayList<Integer>();

  public OSMStats(Path pbf) {
    this.pbf = pbf;
  }

  public void run() {
    Flowable<Osh> flow = RxOshPbfReader //
        .readOsh(pbf, 0, -1, -1) //
        .filter(osh -> osh.getType() == OSMType.RELATION)//
    // .limit(100)

    ;

    subscribe(flow, this::compute, this::error, this::done);

    System.out.println("Average number of Tags per Version: "
        + (double) globalNumberOfTags / globalNumberOfVersions);
    System.out.println(
        "Average number of Tags per Entity: " + (double) globalNumberOfTags / globalNumberOfEntrys);
    System.out.println("number of entrys: " + globalNumberOfEntrys);
    System.out.println("number of all tags: " + globalNumberOfTags);
    System.out.println("number of all Versions: " + globalNumberOfVersions);
    System.out.println("Average number of Versions per Entity: "
        + (double) globalNumberOfVersions / globalNumberOfEntrys);


  }


  /**
   * generalize per Entity!!!
   * 
   * @param versions
   * @return
   */
  private int computeNumberOfTagsNODE(List<Node> versions) {
    int numberOfTags = 0;
    for (Node node : versions) {
      numberOfTags = numberOfTags + node.getTags().length;
    }
    return numberOfTags;
  }

  private int computeNumberOfTagsWAY(List<Way> versions) {
    int numberOfTags = 0;
    for (Way way : versions) {
      numberOfTags = numberOfTags + way.getTags().length;
    }
    return numberOfTags;
  }

  private int computeNumberOfTagsRELATION(List<Relation> versions) {
    int numberOfTags = 0;
    for (Relation relation : versions) {
      numberOfTags = numberOfTags + relation.getTags().length;
    }
    return numberOfTags;
  }



  public void compute(Osh osh) {
    switch (osh.getType()) {
      case NODE:
        nodes(osh.getId(), (List<Node>) (Object) osh.getVersions());
        break;
      case WAY:
        ways(osh.getId(), (List<Way>) (Object) osh.getVersions());
        break;
      case RELATION:
        relations(osh.getId(), (List<Relation>) (Object) osh.getVersions());
        break;
      default:
        System.err.println("unkown osm  type " + osh);
    }
  }


  private void nodes(long id, List<Node> versions) {

    globalNumberOfVersions = globalNumberOfVersions + versions.size();
    globalNumberOfTags = globalNumberOfTags + computeNumberOfTagsNODE(versions);
    globalNumberOfEntrys++;


    // System.out.println(versions);

  }



  private void ways(long id, List<Way> versions) {
    globalNumberOfVersions = globalNumberOfVersions + versions.size();
    globalNumberOfTags = globalNumberOfTags + computeNumberOfTagsWAY(versions);
    globalNumberOfEntrys++;

    // System.out.println(versions);
  }

  private void relations(long id, List<Relation> versions) {
    globalNumberOfVersions = globalNumberOfVersions + versions.size();
    globalNumberOfTags = globalNumberOfTags + computeNumberOfTagsRELATION(versions);
    globalNumberOfEntrys++;

    // System.out.println(versions);
  }


  public void error(Throwable e) {
    // react an error
    e.printStackTrace();
  }

  public void done() {
    // clean up
  }

  public static void main(String[] args) {
    Path dir = Paths.get("C:\\Users\\Danda\\heigit\\data");
    Path pbf;

    pbf = dir.resolve("heidelberg.osh.pbf");
    // pbf = dir.resolve("baden-wuerttemberg.osh.pbf");

    OSMStats osmStats = new OSMStats(pbf);

    Stopwatch stopwatch = Stopwatch.createStarted();
    osmStats.run();
    System.out.println(stopwatch);

  }



  private static <T> void subscribe(Publisher<? extends T> o, final Consumer<? super T> onNext,
      final Consumer<? super Throwable> onError, final Action onComplete) {
    ObjectHelper.requireNonNull(onNext, "onNext is null");
    ObjectHelper.requireNonNull(onError, "onError is null");
    ObjectHelper.requireNonNull(onComplete, "onComplete is null");
    FlowableBlockingSubscribe.subscribe(o,
        new MyLambdaSubscriber<T>(onNext, onError, onComplete, 1L));
  }
}
