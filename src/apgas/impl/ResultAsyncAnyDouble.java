package apgas.impl;

import apgas.Constructs;

public class ResultAsyncAnyDouble extends ResultAsyncAny<Double> {

  private double internalResult;

  public ResultAsyncAnyDouble(double value) {
    this.internalResult = value;
  }

  @Override
  public void mergeResult(ResultAsyncAny<Double> result) {
    System.err.println(
        "[APGAS]: Please use reduceAsyncAnyDouble() for better performance, now using plus as merge operator");
    mergeValue(result.getResult(), Constructs.PLUSDOUBLE);
  }

  @Override
  public void mergeResult(Double result) {
    System.err.println(
        "[APGAS]: Please use reduceAsyncAnyDouble() for better performance, now using plus as merge operator");
    mergeValue(result, Constructs.PLUSDOUBLE);
  }

  @Override
  public void display() {
    System.out.println("ResultAsyncAnyLong " + this.internalResult);
  }

  @Override
  public ResultAsyncAny clone() {
    return new ResultAsyncAnyDouble(this.internalResult);
  }

  @Override
  public Double getResult() {
    return internalResult;
  }

  public void mergeValue(double value, DoubleBinaryOperatorSerializable op) {
    this.internalResult = op.applyAsDouble(internalResult, value);
  }

  public double getInternalResult() {
    return this.internalResult;
  }

  @Override
  public String toString() {
    return "ResultAsyncAnyDouble{" + "internalResult=" + internalResult + '}';
  }
}
