package com.circulosiete.metrics.health.hystrix;

import com.codahale.metrics.health.HealthCheck;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static com.codahale.metrics.health.HealthCheck.Result.healthy;

/**
 * Created by domix on 1/19/17.
 */
public class TestQueryExecutorCommand extends HystrixCommand<HealthCheck.Result> {

  private final DataSource dataSource;
  private final String validationQuery;

  public TestQueryExecutorCommand(DataSource dataSource, String validationQuery, String groupKey) {
    super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(groupKey)));
    this.dataSource = dataSource;
    this.validationQuery = validationQuery;
  }

  @Override
  protected HealthCheck.Result run() throws Exception {
    Connection con = null;
    PreparedStatement pstmt;
    try {
      con = dataSource.getConnection();
      con.setAutoCommit(false);
      pstmt = con.prepareStatement(validationQuery);
      pstmt.executeQuery();

      pstmt.close();

    } finally {
      if (con != null) con.close();
    }

    return healthy();
  }

  @Override
  protected HealthCheck.Result getFallback() {
    return HealthCheck.Result.unhealthy(getExecutionException().getMessage());
  }
}
