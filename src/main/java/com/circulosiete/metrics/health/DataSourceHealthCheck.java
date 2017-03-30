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
package com.circulosiete.metrics.health;

import com.circulosiete.metrics.health.hystrix.TestQueryExecutorCommand;
import com.codahale.metrics.health.HealthCheck;

import javax.sql.DataSource;

import static com.codahale.metrics.health.HealthCheck.Result.healthy;

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
    return
      new TestQueryExecutorCommand(
        dataSource,
        validationQuery,
        "DataSourceHealthCheck")
        .execute();
  }
}
