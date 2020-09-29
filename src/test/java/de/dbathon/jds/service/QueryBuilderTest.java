package de.dbathon.jds.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class QueryBuilderTest {

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
          queryBuilder.withNot(() -> {
            queryBuilder.add("a = 5");
          });
        });
        queryBuilder.withOr(() -> {
          queryBuilder.add("a = 3");
        });
        queryBuilder.withOr(() -> {
          queryBuilder.add("a = 4");
          queryBuilder.add("a = 4");
          queryBuilder.withNot(() -> {
            queryBuilder.add("a = 7");
            queryBuilder.add("a = 8");
          });
        });
        queryBuilder.withNot(() -> {
          queryBuilder.withOr(() -> {
            queryBuilder.add("a = 1");
            queryBuilder.add("a = 2");
          });
        });
      });
      assertEquals(
          "where (a = 1) and (a = 2) and (a = 2) and (not (a = 5)) and (a = 3) "
              + "and ((a = 4) or (a = 4) or (not ((a = 7) and (a = 8)))) and (not ((a = 1) or (a = 2)))",
          queryBuilder.getString());
    }
  }

}
