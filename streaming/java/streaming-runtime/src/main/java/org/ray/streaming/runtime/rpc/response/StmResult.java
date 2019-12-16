package org.ray.streaming.runtime.rpc.response;

import java.io.Serializable;

import com.google.common.base.MoreObjects;

public class StmResult<T> implements Serializable {
  protected T resultObj;
  private boolean success;
  private int resultCode;
  private String resultMsg;

  public StmResult() {
  }

  public StmResult(boolean success, int resultCode, String resultMsg, T resultObj) {
    this.success = success;
    this.resultCode = resultCode;
    this.resultMsg = resultMsg;
    this.resultObj = resultObj;
  }

  public static <T> StmResult<T> success() {
    return new StmResult<>(true, StmResultEnum.SUCCESS.code, StmResultEnum.SUCCESS.msg, null);
  }

  public static <T> StmResult<T> success(T payload) {
    return new StmResult<>(true, StmResultEnum.SUCCESS.code, StmResultEnum.SUCCESS.msg, payload);
  }

  public static <T> StmResult<T> fail() {
    return new StmResult<>(false, StmResultEnum.FAILED.code, StmResultEnum.FAILED.msg, null);
  }

  public static <T> StmResult<T> fail(T payload) {
    return new StmResult<>(false, StmResultEnum.FAILED.code, StmResultEnum.FAILED.msg, payload);
  }

  public static <T> StmResult<T> fail(String msg) {
    return new StmResult<>(false, StmResultEnum.FAILED.code, msg, null);
  }

  public static <T> StmResult<T> fail(StmResultEnum resultEnum, T payload) {
    return new StmResult<>(false, resultEnum.code, resultEnum.msg, payload);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("resultObj", resultObj)
        .add("success", success)
        .add("resultCode", resultCode)
        .add("resultMsg", resultMsg)
        .toString();
  }

  public boolean isSuccess() {
    return this.success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public int getResultCode() {
    return this.resultCode;
  }

  public StmResultEnum getResultEnum() {
    return StmResultEnum.getEnum(this.resultCode);
  }

  public void setResultCode(int resultCode) {
    this.resultCode = resultCode;
  }

  public String getResultMsg() {
    return this.resultMsg;
  }

  public void setResultMsg(String resultMsg) {
    this.resultMsg = resultMsg;
  }

  public T getResultObj() {
    return this.resultObj;
  }

  public void setResultObj(T resultObj) {
    this.resultObj = resultObj;
  }

  public enum StmResultEnum implements Serializable {
    /**
     * stm result enum
     */
    SUCCESS(0, "SUCCESS"),
    FAILED(1, "FAILED");

    public final int code;
    public final String msg;

    private StmResultEnum(int code, String msg) {
      this.code = code;
      this.msg = msg;
    }

    public static StmResultEnum getEnum(int code) {
      for (StmResultEnum value : StmResultEnum.values()) {
        if (code == value.code) {
          return value;
        }
      }
      return FAILED;
    }
  }
}