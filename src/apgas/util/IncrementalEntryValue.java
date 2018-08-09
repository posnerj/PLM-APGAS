package apgas.util;

import java.io.Serializable;

public class IncrementalEntryValue implements Serializable {

  private static final long serialVersionUID = -8035830523372798040L;

  public long uid;

  public IncrementalEntryValue() {
    uid = -1L;
  }
}
