import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.reactive.MyLambdaSubscriber;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;
import org.heigit.bigspatialdata.oshpbf.parser.rx.RxOshPbfReader;
import org.reactivestreams.Publisher;
import crosby.binary.osmosis.OsmosisReader;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

/**
 * @author Alexandra Beikert
 * Reads a PBF File
 * 
 */
public class PBFReader implements Sink{

  public static void main(String[] args) throws FileNotFoundException {
    InputStream inputStream = new FileInputStream("output_existingEntities.pbf");
    OsmosisReader reader = new OsmosisReader(inputStream);
    reader.setSink(new PBFReader());
    reader.run();
   
  }

  @Override
  public void initialize(Map<String, Object> metaData) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void complete() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void process(EntityContainer entityContainer) {
    System.out.println(entityContainer.getEntity().toString());
    // TODO Auto-generated method stub
    
  }

}
