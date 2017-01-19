package com.circulosiete.metrics.health;

import com.codahale.metrics.health.HealthCheck;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static com.codahale.metrics.health.HealthCheck.Result.healthy;
import static com.codahale.metrics.health.HealthCheck.Result.unhealthy;

/**
 * Created by domix on 1/19/17.
 */
public class DataSourceHealthCheck extends HealthCheck {

  private final String validationQuery;
  private final DataSource dataSource;

  public DataSourceHealthCheck(DataSource dataSource, String validationQuery) {
    this.dataSource = dataSource;
    this.validationQuery = validationQuery;
  }

  @Override
  protected Result check() throws Exception {
    Result result;
    if (this.dataSource == null) {
      result = healthy("database unknown");
    } else {
      result = doDataSourceHealthCheck();
    }

    return result;
  }

  private Result doDataSourceHealthCheck() {

    try {
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
    } catch (Exception ex) {
      return unhealthy(ex.getMessage());
    }
  }
}
