package org.ray.api.label;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;
import org.testng.annotations.Test;

public class RayLabelManagerTest {

  @Test
  public void testCreateActorsWithLabels() {
    // we assume there are some basic labels on each ray node, created by ray-deploy when launching ray node.
    // we can set affinities to those basic labels.
    Affinity tfAffinity = new Affinity("EQ", "tf.version", "1.3");
    Affinity gpuAffinity = new Affinity("IN", "GPU", "true");

    Affinity trainingAffinity = new Affinity("EQ", "training", "true");
    Affinity streamingAffinity = new Affinity("EQ", "streaming", "true");
    Affinity mppAffinity = new Affinity("EQ", "mpp", "true");
    Affinity antiTrainingAffinity = new Affinity("NOT_IN", "training", "true");

    Map<String, String> trainingLabels = new HashMap<String, String>() {{
      put("training", "true");
    }};
    Map<String, String> streamingLabels = new HashMap<String, String>() {{
      put("streaming", "true");
    }};
    Map<String, String> mppLabels = new HashMap<String, String>() {{
      put("mpp", "true");
    }};

    // reserve some nodes for training
    Ray.getRuntimeContext().getLabelManager()
        .distributeLabelsToRayNodes(trainingLabels, 2, 5, tfAffinity, gpuAffinity);

    // then reserve nodes for streaming job that prefer to co-locate with training job
    Ray.getRuntimeContext().getLabelManager()
        .distributeLabelsToRayNodes(streamingLabels, 10, 15, trainingAffinity);

    // mpp job want to co-locate with streaming job but anti-locate with training
    Ray.getRuntimeContext().getLabelManager()
        .distributeLabelsToRayNodes(mppLabels, 5, 10, streamingAffinity, antiTrainingAffinity);

    // prepare creation affinity for training
    ActorCreationOptions trainingOptions = new ActorCreationOptions.Builder()
        .setAffinities(Collections.singletonList(trainingAffinity))
        .createActorCreationOptions();

    // prepare creation affinity for streaming
    ActorCreationOptions streamingOptions = new ActorCreationOptions.Builder()
        .setAffinities(Collections.singletonList(streamingAffinity))
        .createActorCreationOptions();

    // prepare creation affinity for mpp
    ActorCreationOptions mppOptions = new ActorCreationOptions.Builder()
        .setAffinities(Collections.singletonList(mppAffinity))
        .createActorCreationOptions();

    // create actors / actor group
    Ray.createActor(TrainActor::new, trainingOptions);
    Ray.createActor(StreamingActor::new, streamingOptions);
    Ray.createActor(MppActor::new, mppOptions);
  }

  @RayRemote
  public static class TrainActor {

    int counter = 0;

    public int inc() {
      return ++counter;
    }
  }

  @RayRemote
  public static class StreamingActor {

    int counter = 0;

    public int inc() {
      return ++counter;
    }
  }

  @RayRemote
  public static class MppActor {

    int counter = 0;

    public int inc() {
      return ++counter;
    }
  }
}
