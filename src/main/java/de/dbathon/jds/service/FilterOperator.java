package de.dbathon.jds.service;

import static de.dbathon.jds.util.JsonUtil.toJsonString;
import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.dbathon.jds.util.JsonMap;
import de.dbathon.jds.util.JsonStringNumber;
import de.dbathon.jds.util.JsonUtil;

public abstract class FilterOperator {

  public static final Map<String, FilterOperator> FILTER_OPERATORS;

  static {
    final Map<String, FilterOperator> filterOperators = new HashMap<>();
    filterOperators.put("=", new EqualsViaContainsOperator(String.class, JsonStringNumber.class, Boolean.class, null));
    filterOperators.put("!=", new SimpleOperator("=", true, String.class, JsonStringNumber.class, Boolean.class, null));

    filterOperators.put("<", new SimpleOperator("<", false, String.class, JsonStringNumber.class));
    filterOperators.put("<=", new SimpleOperator("<=", false, String.class, JsonStringNumber.class));
    filterOperators.put(">", new SimpleOperator(">", false, String.class, JsonStringNumber.class));
    filterOperators.put(">=", new SimpleOperator(">=", false, String.class, JsonStringNumber.class));

    filterOperators.put("is", new IsTypeOperator());

    filterOperators.put("in", new InOperator());

    // TODO: prefix (mainly for id), like, ilike

    FILTER_OPERATORS = Collections.unmodifiableMap(filterOperators);
  }

  /**
   * TODO: allow more characters in names
   */
  protected static final String VALID_NAME_PATTERN_STRING = "[a-zA-Z0-9_\\-]+";
  protected static final String VALID_INDEX_PATTERN_STRING = "\\[(?:[1-9][0-9]{0,8}|0)\\]";

  protected static final Pattern VALID_KEY_PATTERN = Pattern.compile(
      VALID_NAME_PATTERN_STRING + "(?:\\." + VALID_NAME_PATTERN_STRING + "|" + VALID_INDEX_PATTERN_STRING + ")*");

  protected static final Pattern NAME_OR_INDEX_PATTERN =
      Pattern.compile(VALID_NAME_PATTERN_STRING + "|" + VALID_INDEX_PATTERN_STRING);

  public abstract void apply(final QueryBuilder queryBuilder, final String key, final Object rightHandSide);

  protected static boolean isSpecialKey(final String key) {
    return DocumentService.ID_PROPERTY.equals(key) || DocumentService.VERSION_PROPERTY.equals(key);
  }

  protected String getJsonPathExpression(final String key) {
    if (!VALID_KEY_PATTERN.matcher(key).matches()) {
      throw new ApiException("invalid filter key: " + key);
    }
    final StringBuilder result = new StringBuilder("data");
    final Matcher matcher = NAME_OR_INDEX_PATTERN.matcher(key);
    while (matcher.find()) {
      final String match = matcher.group();
      if (match.startsWith("[")) {
        result.append("->").append(match.substring(1, match.length() - 1));
      }
      else {
        // no need for escaping, the pattern allows only save parameters
        result.append("->'").append(match).append("'");
      }
    }
    return result.toString();
  }

  private static class SimpleOperator extends FilterOperator {

    private final String operator;
    private final boolean negate;
    private final Class<?>[] allowedTypes;

    public SimpleOperator(final String operator, final boolean negate, final Class<?>... allowedTypes) {
      this.operator = operator;
      this.negate = negate;
      this.allowedTypes = allowedTypes;
    }

    protected boolean isTypeAllowed(final Object rightHandSide) {
      for (final Class<?> allowedType : allowedTypes) {
        if ((allowedType != null && allowedType.isInstance(rightHandSide))
            || (allowedType == null && rightHandSide == null)) {
          return true;
        }
      }
      return false;
    }

    protected void addToQueryBuilder(final QueryBuilder queryBuilder, final String expression,
        final Object... parameters) {
      /**
       * We don't want null to propagate in logical expressions, mainly because "not null" is null,
       * but we would like it to be "true", so always coalesce it to false.
       */
      queryBuilder.add((negate ? "not coalesce(" : "coalesce(") + expression + ", false)", parameters);
    }

    protected void applyNonSpecialKey(final QueryBuilder queryBuilder, final String key, final Object rightHandSide) {
      final StringBuilder builder = new StringBuilder();
      final String pathExpression = getJsonPathExpression(key);
      builder.append(pathExpression).append(" ");
      final String parameter;

      final boolean isEqualsOperator = "=".equals(operator);
      if (!isEqualsOperator && rightHandSide instanceof String) {
        /**
         * For comparisons of strings use the "C" collation to have predictable results independent
         * of the default collation of the database.
         */
        builder.append("#>> '{}' ").append(operator).append(" (? collate \"C\")");
        parameter = (String) rightHandSide;
      }
      else {
        builder.append(operator).append(" ?::jsonb");
        parameter = toJsonString(rightHandSide);
      }

      if (!isEqualsOperator) {
        /**
         * For all operators except "=" also make sure the type matches, postgres will compare
         * numbers with strings etc. but we don't really want that, if the types do not match, then
         * it should be false.
         */
        final String expectedType;
        if (rightHandSide instanceof String) {
          expectedType = "string";
        }
        else if (rightHandSide instanceof JsonStringNumber) {
          expectedType = "number";
        }
        else {
          throw new IllegalStateException(
              "unexpected right ahand side for operator " + operator + ": " + rightHandSide);
        }

        builder.append(" and jsonb_typeof(").append(pathExpression).append(") = '").append(expectedType).append("'");
      }

      addToQueryBuilder(queryBuilder, builder.toString(), parameter);
    }

    @Override
    public void apply(final QueryBuilder queryBuilder, final String key, final Object rightHandSide) {
      if (!isTypeAllowed(rightHandSide)) {
        throw new ApiException("invalid right hand side for operator " + operator + ": " + toJsonString(rightHandSide));
      }
      if (isSpecialKey(key)) {
        if (rightHandSide instanceof String) {
          addToQueryBuilder(queryBuilder, key + " " + operator + " ?", rightHandSide);
        }
        else {
          // else it is false (the special keys are always string not null)
          queryBuilder.add(negate ? "true" : "false");
        }
      }
      else {
        applyNonSpecialKey(queryBuilder, key, rightHandSide);
      }
    }

  }

  private static class EqualsViaContainsOperator extends SimpleOperator {

    public EqualsViaContainsOperator(final Class<?>... allowedTypes) {
      super("=", false, allowedTypes);
    }

    private JsonMap buildContainsValue(final String key, final Object rightHandSide) {
      if (!VALID_KEY_PATTERN.matcher(key).matches()) {
        throw new ApiException("invalid filter key: " + key);
      }
      final JsonMap result = new JsonMap();
      JsonMap lastMap = result;
      String lastKey = null;

      final Matcher matcher = NAME_OR_INDEX_PATTERN.matcher(key);
      while (matcher.find()) {
        final String nextKey = matcher.group();
        if (lastKey != null) {
          final JsonMap newMap = new JsonMap();
          lastMap.put(lastKey, newMap);
          lastMap = newMap;
        }
        lastKey = nextKey;
      }

      requireNonNull(lastKey);
      lastMap.put(lastKey, rightHandSide);

      return result;
    }

    @Override
    protected void applyNonSpecialKey(final QueryBuilder queryBuilder, final String key, final Object rightHandSide) {
      if (key.indexOf('[') >= 0) {
        /**
         * Array paths are currently not supported here, just use the default behavior.
         */
        super.applyNonSpecialKey(queryBuilder, key, rightHandSide);
      }
      else {
        // we can use the jsonb_path_ops index to optimize this
        addToQueryBuilder(queryBuilder, "data @> ?::jsonb", toJsonString(buildContainsValue(key, rightHandSide)));
      }
    }

  }

  private static class IsTypeOperator extends FilterOperator {

    private static final Set<String> EXPECTED_TYPES =
        new HashSet<>(Arrays.asList("object", "array", "string", "number", "boolean", "null", "undefined"));

    @Override
    public void apply(final QueryBuilder queryBuilder, final String key, final Object rightHandSide) {
      if (!EXPECTED_TYPES.contains(rightHandSide)) {
        throw new ApiException("unexpected type for \"is\" operator: " + toJsonString(rightHandSide));
      }

      if (isSpecialKey(key)) {
        // both special keys are always string
        queryBuilder.add(Boolean.valueOf("string".equals(rightHandSide)).toString());
      }
      else {
        queryBuilder.add("coalesce(jsonb_typeof(" + getJsonPathExpression(key) + "), 'undefined') = ?", rightHandSide);
      }
    }

  }

  private static class InOperator extends FilterOperator {

    private boolean handleEmpty(final QueryBuilder queryBuilder, final List<?> arguments) {
      if (arguments.isEmpty()) {
        // if there are no arguments, then there is no match
        queryBuilder.add("false");
        return true;
      }
      return false;
    }

    protected String generateParameters(final String singleParameter, final int size) {
      return IntStream.range(0, size).mapToObj(i -> singleParameter).collect(Collectors.joining(", "));
    }

    @Override
    public void apply(final QueryBuilder queryBuilder, final String key, final Object rightHandSide) {
      if (!(rightHandSide instanceof List<?>)) {
        throw new ApiException("invalid right hand side for \"in\" operator: " + toJsonString(rightHandSide));
      }
      final List<?> arguments = (List<?>) rightHandSide;
      if (arguments.size() > 1000) {
        throw new ApiException("too many arguments for \"in\" operator: " + arguments.size());
      }

      if (isSpecialKey(key)) {
        final List<?> stringArguments =
            arguments.stream().filter(argument -> argument instanceof String).collect(Collectors.toList());

        if (handleEmpty(queryBuilder, stringArguments)) {
          return;
        }

        queryBuilder.add(key + " in (" + generateParameters("?", stringArguments.size()) + ")", stringArguments);
      }
      else {
        if (handleEmpty(queryBuilder, arguments)) {
          return;
        }

        queryBuilder.add(getJsonPathExpression(key) + " in (" + generateParameters("?::jsonb", arguments.size()) + ")",
            arguments.stream().map(JsonUtil::toJsonString).collect(Collectors.toList()));
      }
    }

  }

}
