package org.ray.faulttolerance;

public interface CheckpointableActor {

  /**
   * Save a checkpoint.
   *
   * @param checkpointId Id of the checkpoint.
   * @return Wether the checkpoint was successfully saved.
   */
  boolean saveCheckpoint(long checkpointId);

  /**
   * Load a checkpoint and resume actor state.
   *
   * @param checkpointId Id of the checkpoint.
   * @return Whether the actor was successfully resumed from the checkpoint.
   */
  boolean loadCheckpoint(long checkpointId);
}

