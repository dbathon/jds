package de.dbathon.jds.persistence;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

public class RuntimeSqlException extends RuntimeException {

  public RuntimeSqlException(final String message, final SQLException cause) {
    super(message, cause);
  }

  public RuntimeSqlException(final SQLException cause) {
    super(cause);
  }

  public SQLException getSqlException() {
    return (SQLException) getCause();
  }

  public boolean isIntegrityContraintViolation() {
    if (getSqlException() instanceof SQLIntegrityConstraintViolationException) {
      return true;
    }
    // if it is not a SQLIntegrityConstraintViolationException then check the SQL state
    final String sqlState = getSqlException().getSQLState();
    return sqlState != null && sqlState.startsWith("23");
  }

}
