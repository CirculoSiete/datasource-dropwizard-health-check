/**
 *
 * Copyright (C) 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    return healthy("DB-OK");
  }

  @Override
  protected HealthCheck.Result getFallback() {
    return HealthCheck.Result.unhealthy(getExecutionException().getMessage());
  }
}
