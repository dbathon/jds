package de.dbathon.jds.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.sql.DataSource;

/**
 * Provides access to the {@link Connection database connection} for the current transaction. Also
 * implements various useful methods using the connection.
 * <p>
 * No method will throw {@link SQLException}, instead the exceptions will be wrapped in
 * {@link RuntimeSqlException}.
 */
@ApplicationScoped
public class DatabaseConnection {

  @Inject
  DataSource dataSource;

  public interface FunctionWithConnection<T> {
    T apply(Connection connection) throws SQLException;
  }

  public <T> T withConnection(final FunctionWithConnection<T> function) {
    try (Connection connection = dataSource.getConnection()) {
      return function.apply(connection);
    }
    catch (final SQLException e) {
      throw new RuntimeSqlException(e);
    }
  }

  private void bindParameters(final PreparedStatement preparedStatement, final Object... parameters)
      throws SQLException {
    if (parameters != null) {
      for (int i = 0; i < parameters.length; ++i) {
        final Object parameter = parameters[i];
        if (parameter instanceof String) {
          preparedStatement.setString(i + 1, (String) parameter);
        }
        else if (parameter instanceof Long) {
          preparedStatement.setLong(i + 1, (Long) parameter);
        }
        else if (parameter instanceof Integer) {
          preparedStatement.setInt(i + 1, (Integer) parameter);
        }
        else if (parameter instanceof Boolean) {
          preparedStatement.setBoolean(i + 1, (Boolean) parameter);
        }
        else {
          throw new IllegalArgumentException("unsupported parameter value: " + parameter);
        }
      }
    }
  }

  private <T> T getColumnValue(final ResultSet resultSet, final int columnIndex, final Class<T> type)
      throws SQLException {
    if (type == String.class) {
      return type.cast(resultSet.getString(columnIndex));
    }
    else if (type == Long.class) {
      final long value = resultSet.getLong(columnIndex);
      return type.cast(resultSet.wasNull() ? null : value);
    }
    else if (type == Integer.class) {
      final int value = resultSet.getInt(columnIndex);
      return type.cast(resultSet.wasNull() ? null : value);
    }
    else if (type == Boolean.class) {
      final boolean value = resultSet.getBoolean(columnIndex);
      return type.cast(resultSet.wasNull() ? null : value);
    }
    else {
      throw new IllegalArgumentException("unsupported type: " + type);
    }
  }

  private <T> List<T> executeQuery(final PreparedStatement preparedStatement, final Class<T> rowType,
      final Class<?>[] columnTypes) throws SQLException {
    final List<T> result = new ArrayList<>();
    try (ResultSet resultSet = preparedStatement.executeQuery()) {
      ResultSetMetaData metaData = null;
      int columnCount = columnTypes != null ? columnTypes.length : 0;
      while (resultSet.next()) {
        if (rowType == Object[].class) {
          if (columnTypes == null) {
            if (metaData == null) {
              metaData = resultSet.getMetaData();
              columnCount = metaData.getColumnCount();
            }
            final Object[] row = new Object[columnCount];
            for (int i = 0; i < columnCount; ++i) {
              row[i] = resultSet.getObject(i + 1);
            }
            result.add(rowType.cast(row));
          }
          else {
            // use the given columnTypes
            final Object[] row = new Object[columnCount];
            for (int i = 0; i < columnCount; ++i) {
              row[i] = getColumnValue(resultSet, i + 1, columnTypes[i]);
            }
            result.add(rowType.cast(row));
          }
        }
        else {
          result.add(getColumnValue(resultSet, 1, rowType));
        }
      }
    }
    return result;
  }

  public <T> List<T> query(final String sql, final Class<T> rowType, final Class<?>[] columnTypes,
      final Object... parameters) {
    return withConnection(connection -> {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        bindParameters(preparedStatement, parameters);
        return executeQuery(preparedStatement, rowType, columnTypes);
      }
    });
  }

  public <T> List<T> query(final String sql, final Class<T> rowType, final Object... parameters) {
    return query(sql, rowType, (Class<?>[]) null, parameters);
  }

  public <T> T queryNoOrOneResult(final String sql, final Class<T> rowType, final Class<?>[] columnTypes,
      final Object... parameters) {
    final List<T> results = query(sql, rowType, columnTypes, parameters);
    if (results.isEmpty()) {
      return null;
    }
    else if (results.size() == 1) {
      return results.get(0);
    }
    else {
      throw new IllegalStateException("more than one result (" + results.size() + " for query: " + sql);
    }
  }

  public <T> T queryNoOrOneResult(final String sql, final Class<T> rowType, final Object... parameters) {
    return queryNoOrOneResult(sql, rowType, (Class<?>[]) null, parameters);
  }

  public int executeUpdate(final String sql, final Object... parameters) {
    return withConnection(connection -> {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        bindParameters(preparedStatement, parameters);
        return preparedStatement.executeUpdate();
      }
    });
  }

}
