package org.heigit.ohsome.oshdb.api.mapreducer.backend;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.heigit.ohsome.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.Kernels.CancelableProcessStatus;

/**
 * A cancelable ignite broadcast task.
 *
 * @param <T> Type of the task argument.
 * @param <R> Type of the task result returning from {@link ComputeTask#reduce(List)} method.
 */
@org.apache.ignite.compute.ComputeTaskNoResultCache
class OSHDBIgniteMapReduceComputeTask<T, R> extends ComputeTaskAdapter<T, R>
    implements Serializable {
  interface CancelableIgniteMapReduceJob<S> extends Serializable, CancelableProcessStatus {
    void cancel();

    S execute(Ignite node);
  }

  private final CancelableIgniteMapReduceJob job;
  private final SerializableBinaryOperator<R> combiner;
  private final IgniteRunnable onClose;

  private R resultAccumulator;

  public OSHDBIgniteMapReduceComputeTask(
      CancelableIgniteMapReduceJob job,
      SerializableSupplier<R> identitySupplier,
      SerializableBinaryOperator<R> combiner,
      IgniteRunnable onClose
  ) {
    this.job = job;
    this.combiner = combiner;
    this.resultAccumulator = identitySupplier.get();
    this.onClose = onClose;
  }

  @Override
  public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, T arg)
      throws IgniteException {
    Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());
    subgrid.forEach(node -> map.put(new ComputeJob() {
      @IgniteInstanceResource
      private Ignite ignite;

      @Override
      public void cancel() {
        job.cancel();
      }

      @Override
      public Object execute() throws IgniteException {
        Object result = job.execute(ignite);
        onClose.run();
        return result;
      }
    }, node));
    return map;
  }

  @Override
  public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd)
      throws IgniteException {
    R data = res.getData();
    resultAccumulator = combiner.apply(resultAccumulator, data);
    return ComputeJobResultPolicy.WAIT;
  }

  @Override
  public R reduce(List<ComputeJobResult> results) throws IgniteException {
    return resultAccumulator;
  }
}
