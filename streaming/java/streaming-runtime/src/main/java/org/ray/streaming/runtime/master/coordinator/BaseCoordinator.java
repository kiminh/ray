package org.ray.streaming.runtime.master.coordinator;

import com.alipay.streaming.runtime.graphmanager.GraphManager;
import com.alipay.streaming.runtime.master.JobMaster;
import com.alipay.streaming.runtime.master.JobMasterRuntimeContext;
import com.alipay.streaming.runtime.metrics.MasterMetrics;
import com.alipay.streaming.runtime.utils.LoggerFactory;
import org.ray.api.Ray;
import org.slf4j.Logger;

public abstract class BaseCoordinator implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseCoordinator.class);

  protected final JobMaster jobMaster;

  protected final JobMasterRuntimeContext runtimeContext;
  protected final GraphManager graphManager;
  protected final MasterMetrics metrics;

  private Thread t;
  protected volatile boolean closed;

  public BaseCoordinator(JobMaster jobMaster) {
    this.jobMaster = jobMaster;
    this.runtimeContext = jobMaster.getRuntimeContext();
    this.graphManager = jobMaster.getGraphManager();
    this.metrics = jobMaster.getMetrics();
  }

  public void start() {
    t = new Thread(Ray.wrapRunnable(this),
        this.getClass().getName() + "-" + System.currentTimeMillis());
    t.start();
  }

  public void stop() {
    closed = true;

    try {
      if (t != null) {
        t.join(30000);
      }
    } catch (InterruptedException e) {
      LOG.error("Coordinator thread exit has exception.", e);
    }
  }
}
