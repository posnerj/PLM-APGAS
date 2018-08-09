package apgas.util;

import static apgas.Constructs.places;

import apgas.Place;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ResilientPlaceGroup implements Serializable {

  private final Place[] array;
  private int max;

  public ResilientPlaceGroup(int size) {
    array = new Place[size];
    int id = 0;
    for (final Place p : places()) {
      array[id++] = p;
      if (id == size) {
        max = p.id;
        return;
      }
    }
    System.err.println("[APGAS] Too few places to initialize the ResilientPlaceGroup. Aborting.");
    System.exit(1);
  }

  public void fix() {
    final List<? extends Place> places = places();
    final Iterator<? extends Place> it = places.iterator();
    Place spare;
    try {
      for (int id = 0; id < array.length; ++id) {
        if (!places.contains(array[id])) {
          for (; ; ) {
            spare = it.next();
            if (spare.id > max) {
              break;
            }
          }
          array[id] = spare;
          max = spare.id;
        }
      }
    } catch (final NoSuchElementException e) {
      System.err.println("[APGAS] Too few places to fix the ResilientPlaceGroup. Aborting.");
      System.exit(1);
    }
  }

  public Place get(int id) {
    return array[id];
  }

  public int size() {
    return array.length;
  }

  public boolean contains(Place place) {
    for (int id = 0; id < array.length; ++id) {
      if (array[id].id == place.id) {
        return true;
      }
    }
    return false;
  }

  public int indexOf(Place place) {
    for (int id = 0; id < array.length; ++id) {
      if (array[id].id == place.id) {
        return id;
      }
    }
    return -1;
  }

  public List<Place> asList() {
    return List.of(array);
  }
}
