package de.dbathon.jds.rest;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Simple CORS filter that basically allows everything from any origin. It also directly handles
 * OPTIONS requests.
 */
@WebFilter(urlPatterns = "/*")
public class CorsFilter implements Filter {

  private static final String ALLOWED_METHODS = "GET,POST,PUT,DELETE,HEAD,OPTIONS";
  private static final String ALLOWED_HEADERS = "origin,accept,content-type,authorization";

  @Override
  public void init(final FilterConfig filterConfig) throws ServletException {}

  @Override
  public void destroy() {}

  @Override
  public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
      throws IOException, ServletException {

    final HttpServletRequest httpRequest = (HttpServletRequest) request;
    final HttpServletResponse httpResponse = (HttpServletResponse) response;

    final String origin = httpRequest.getHeader("Origin");

    if (origin != null && !origin.isEmpty()) {
      httpResponse.addHeader("Access-Control-Allow-Origin", origin);
      httpResponse.addHeader("Access-Control-Allow-Credentials", "true");
      httpResponse.addHeader("Access-Control-Allow-Methods", ALLOWED_METHODS);
      httpResponse.addHeader("Access-Control-Allow-Headers", ALLOWED_HEADERS);
      httpResponse.addHeader("Access-Control-Max-Age", "86400");
    }

    if ("OPTIONS".equals(httpRequest.getMethod())) {
      // just return 200 OK
      httpResponse.addHeader("Allow", ALLOWED_METHODS);
      httpResponse.setStatus(HttpServletResponse.SC_OK);
    }
    else {
      chain.doFilter(request, response);
    }
  }

}
