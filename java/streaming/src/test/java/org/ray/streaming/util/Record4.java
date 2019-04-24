package org.ray.streaming.util;

import java.io.Serializable;

public class Record4 implements Serializable {
  private Long f0;
  private Long f1;
  private Long f2;
  private Long f3;

  public Record4(Long f0, Long f1, Long f2, Long f3) {
    this.f0 = f0;
    this.f1 = f1;
    this.f2 = f2;
    this.f3 = f3;
  }

  public Long getF0() {
    return f0;
  }

  public void setF0(Long f0) {
    this.f0 = f0;
  }

  public Long getF1() {
    return f1;
  }

  public void setF1(Long f1) {
    this.f1 = f1;
  }

  public Long getF2() {
    return f2;
  }

  public void setF2(Long f2) {
    this.f2 = f2;
  }

  public Long getF3() {
    return f3;
  }

  public void setF3(Long f3) {
    this.f3 = f3;
  }
}
