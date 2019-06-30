package de.dbathon.jds.service;

import static java.util.Objects.requireNonNull;

import javax.ws.rs.core.Response.Status;

import de.dbathon.jds.rest.AnyExceptionMapper;

/**
 * Will be mapped to an appropriate response using {@link #getStatus()} and {@link #getMessage()} by
 * {@link AnyExceptionMapper}.
 */
public class ApiException extends RuntimeException {

  private final Status status;

  public ApiException(final String message, final Status status) {
    super(message);
    this.status = requireNonNull(status);
  }

  public ApiException(final String message, final Throwable cause, final Status status) {
    super(message, cause);
    this.status = requireNonNull(status);
  }

  public ApiException(final String message) {
    this(message, Status.BAD_REQUEST);
  }

  public ApiException(final String message, final Throwable cause) {
    this(message, cause, Status.BAD_REQUEST);
  }

  public Status getStatus() {
    return status;
  }

}
