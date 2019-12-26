package org.ray.api.label;

import java.util.Arrays;

/**
 * Affinity are used to pick target nodes in two cases:
 * 1) when distributing labels to nodes.
 * 2) when matching nodes for actors.
 *
 * It works with node's labels (represented by a Map[String, String])
 */
public class Affinity {

  /**
   * matching strategy: enforce or prefer.
   */
  public final boolean enforce = false;

  /**
   * matching expression operator: EQ, IN, NOT_IN
   */
  public final String operator;

  public final String key;

  public final String[] values;

  public Affinity(String operator, String key, String... values) {
    this.operator = operator;
    this.key = key;
    this.values = values;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Affinity{");
    sb.append("enforce=").append(enforce);
    sb.append(", operator='").append(operator).append('\'');
    sb.append(", key='").append(key).append('\'');
    sb.append(", values=").append(Arrays.toString(values));
    sb.append('}');
    return sb.toString();
  }

}
