package de.dbathon.jds.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class QueryBuilderTest {

  @Test
  void test() {
    {
      final QueryBuilder queryBuilder = new QueryBuilder();
      queryBuilder.add("where");
      queryBuilder.withAnd(() -> {
        queryBuilder.add("a = 1");
      });
      assertEquals("where a = 1", queryBuilder.getString());
    }

    {
      final QueryBuilder queryBuilder = new QueryBuilder();
      queryBuilder.add("where");
      queryBuilder.withAnd(() -> {
        queryBuilder.add("a = 1");
        queryBuilder.withAnd(() -> {
          queryBuilder.add("a = 2");
          queryBuilder.add("a = 2");
        });
        queryBuilder.withOr(() -> {
          queryBuilder.add("a = 3");
        });
        queryBuilder.withOr(() -> {
          queryBuilder.add("a = 4");
          queryBuilder.add("a = 4");
        });
      });
      assertEquals("where (a = 1) and (a = 2) and (a = 2) and (a = 3) and ((a = 4) or (a = 4))",
          queryBuilder.getString());
    }
  }

}
