/**
 * Alipay.com Inc. Copyright (c) 2004-2019 All Rights Reserved.
 */
package org.ray.streaming.runtime.core.graph.executiongraph;

/**
 * Operator node for rescale request transformer.
 *
 * @author buhe
 * @version $Id: OperatorNode.java, v 0.1 2019/09/24 21:36 buhe Exp $
 */
public class OperatorNode {

  /**
   * operator index, like "1" which corresponds to rest api request parameter operator name
   */
  private String opIndex;
  /**
   * operator name, like "1-SourceOperator", "4-ReduceOperator"
   */
  private String opName;
  /**
   * operator parallelism number
   */
  private int parallelism;

  public OperatorNode() {
  }

  public OperatorNode(String opIndex, String opName, int parallelism) {
    this.opIndex = opIndex;
    this.opName = opName;
    this.parallelism = parallelism;
  }

  @Override
  public String toString() {
    return "OperatorNode{" +
        "opIndex='" + opIndex + '\'' +
        ", opName='" + opName + '\'' +
        ", parallelism=" + parallelism +
        '}';
  }

  public String getOpIndex() {
    return opIndex;
  }

  public void setOpIndex(String opIndex) {
    this.opIndex = opIndex;
  }

  public String getOpName() {
    return opName;
  }

  public void setOpName(String opName) {
    this.opName = opName;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }
}