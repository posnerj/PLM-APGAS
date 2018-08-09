package apgas.impl;

import java.io.Serializable;

public abstract class ResultAsyncAny<T extends Serializable> implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  protected T result;

  public abstract void mergeResult(ResultAsyncAny<T> result);

  public abstract void mergeResult(T result);

  public abstract void display();

  public abstract ResultAsyncAny clone();

  public T getResult() {
    return result;
  }
}
