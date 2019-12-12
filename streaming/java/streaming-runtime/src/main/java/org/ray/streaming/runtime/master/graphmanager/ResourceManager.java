package org.ray.streaming.runtime.master.graphmanager;

import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.streaming.runtime.worker.JobWorker2;

public class ResourceManager {

  public List<RayActor<JobWorker2>> createWorker(int workerNum) {
    List<RayActor<JobWorker2>> workers = new ArrayList<>();
    for (int i = 0; i < workerNum; i++) {
      RayActor<JobWorker2> worker = Ray.createActor(JobWorker2::new);
      workers.add(worker);
    }
    return workers;
  }

}
