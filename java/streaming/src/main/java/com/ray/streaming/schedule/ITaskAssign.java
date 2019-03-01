package com.ray.streaming.schedule;

import com.ray.streaming.core.graph.ExecutionGraph;
import com.ray.streaming.core.runtime.StreamWorker;
import com.ray.streaming.plan.Plan;
import java.util.List;
import org.ray.api.RayActor;


public interface ITaskAssign {

  ExecutionGraph assign(Plan plan, List<RayActor<StreamWorker>> workers);
}
