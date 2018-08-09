package apgas.impl;

import apgas.Constructs;

public class ResultAsyncAnyLong extends ResultAsyncAny<Long> {

  private long internalResult;

  public ResultAsyncAnyLong(long value) {
    this.internalResult = value;
  }

  @Override
  public void mergeResult(ResultAsyncAny<Long> result) {
    System.err.println(
        "[APGAS]: Please use reduceAsyncAnyLong() for better performance, now using plus as merge operator");
    mergeValue(result.getResult(), Constructs.PLUSLONG);
  }

  @Override
  public void mergeResult(Long result) {
    System.err.println(
        "[APGAS]: Please use reduceAsyncAnyLong() for better performance, now using plus as merge operator");
    mergeValue(result, Constructs.PLUSLONG);
  }

  @Override
  public void display() {
    System.out.println("ResultAsyncAnyLong " + this.internalResult);
  }

  @Override
  public ResultAsyncAny clone() {
    return new ResultAsyncAnyLong(this.internalResult);
  }

  @Override
  public Long getResult() {
    return internalResult;
  }

  public void mergeValue(long value, LongBinaryOperatorSerializable op) {
    this.internalResult = op.applyAsLong(internalResult, value);
  }

  public long getInternalResult() {
    return this.internalResult;
  }

  @Override
  public String toString() {
    return "ResultAsyncAnyLong{" + "internalResult=" + internalResult + '}';
  }
}
