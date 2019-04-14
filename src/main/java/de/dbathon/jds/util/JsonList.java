package de.dbathon.jds.util;

import java.util.ArrayList;

/**
 * An {@link ArrayList} of {@link Object} that has an {@linkplain #addElement(Object) add method}
 * that can be chained.
 */
public class JsonList extends ArrayList<Object> {

  public JsonList addElement(final Object element) {
    add(element);
    return this;
  }

}
