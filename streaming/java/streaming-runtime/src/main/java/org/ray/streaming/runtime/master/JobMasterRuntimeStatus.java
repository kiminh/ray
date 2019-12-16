package org.ray.streaming.runtime.master;

public enum JobMasterRuntimeStatus {

  /**
   * None
   */
  NONE("ADAPTION_NONE", 0),

  /**
   * Normal rescaling request
   */
  RESCALING_REQUEST("RESCALING_REQUEST", 1),

  /**
   * Normal rescaling progress
   */
  RESCALING_PROGRESS("RESCALING_PROGRESS", 2),

  /**
   * Normal migration
   */
  MIGRATION("MIGRATION", 4),

  /**
   * Normal updating
   */
  UPDATING("UPDATING", 5),

  /**
   * Adaption rescaling request
   */
  ADAPTION_RESCALING_REQUEST("ADAPTION_RESCALING_REQUEST", 11),

  /**
   * Adaption rescaling progress
   */
  ADAPTION_RESCALING_PROGRESS("ADAPTION_RESCALING_PROGRESS", 12),

  /**
   * Adaption migration
   */
  ADAPTION_MIGRATION("ADAPTION_MIGRATION", 21),

  /**
   * Adaption updating
   */
  ADAPTION_UPDATING("ADAPTION_UPDATING", 31);

  private String name;
  private int index;

  JobMasterRuntimeStatus(String name, int index) {
    this.name = name;
    this.index = index;
  }
}
