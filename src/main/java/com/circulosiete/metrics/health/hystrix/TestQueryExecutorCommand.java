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
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static com.codahale.metrics.health.HealthCheck.Result.healthy;
import static java.util.Optional.ofNullable;
import static org.apache.commons.lang.exception.ExceptionUtils.getCause;

/**
 * Created by domix on 1/19/17.
 */
@Slf4j
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
    log.debug("Running the HealthCheck");

    try (Connection con = dataSource.getConnection();
         PreparedStatement pstmt = con.prepareStatement(validationQuery)) {
      pstmt.executeQuery();
      log.debug("DB is healthy");
    }
    return healthy("DB-OK");
  }

  @Override
  protected HealthCheck.Result getFallback() {
    log.debug("DB is unhealthy");
    return ofNullable(getCause(getExecutionException()))
      .map(HealthCheck.Result::unhealthy)
      .orElse(HealthCheck.Result.unhealthy("Fail"));
  }
}
