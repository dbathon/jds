package de.dbathon.jds.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class QueryBuilder {

  private StringBuilder stringBuilder = new StringBuilder();

  private final List<Object> parameters = new ArrayList<>();

  private String currentOperator;

  private int addCount = 0;

  public void add(final String expression, final Object... parameters) {
    if (addCount > 0) {
      stringBuilder.append(" ");
      if (currentOperator != null) {
        stringBuilder.append(currentOperator).append(" ");
      }
    }

    if (currentOperator != null) {
      stringBuilder.append("(");
    }
    stringBuilder.append(expression);
    if (currentOperator != null) {
      stringBuilder.append(")");
    }

    if (parameters != null && parameters.length > 0) {
      this.parameters.addAll(Arrays.asList(parameters));
    }
    ++addCount;
  }

  public void add(final String expression) {
    add(expression, (Object[]) null);
  }

  private void withOperator(final String operator, final Runnable runnable) {
    if (operator.equals(currentOperator)) {
      // just run the runnable
      runnable.run();
    }
    else {
      final StringBuilder outerStringBuilder = stringBuilder;
      final String previousOperator = currentOperator;
      final int previousAddCount = addCount;
      try {
        stringBuilder = new StringBuilder();
        currentOperator = operator;
        addCount = 0;
        runnable.run();
      }
      finally {
        String string;
        if (addCount == 0) {
          // nothing to do
          string = null;
        }
        else {
          string = stringBuilder.toString();
          if (addCount == 1) {
            // strip the parentheses, the add call below will add them again if necessary
            string = string.substring(1, string.length() - 1);
          }
        }
        stringBuilder = outerStringBuilder;
        currentOperator = previousOperator;
        addCount = previousAddCount;

        if (string != null) {
          add(string);
        }
      }
    }
  }

  public void withAnd(final Runnable runnable) {
    withOperator("and", runnable);
  }

  public void withOr(final Runnable runnable) {
    withOperator("or", runnable);
  }

  private void checkNoOperator() {
    if (currentOperator != null) {
      throw new IllegalStateException("operator active");
    }
  }

  public String getString() {
    checkNoOperator();
    return stringBuilder.toString();
  }

  public List<Object> getParameters() {
    checkNoOperator();
    return Collections.unmodifiableList(parameters);
  }

  public Object[] getParametersArray() {
    checkNoOperator();
    return parameters.toArray();
  }

}
