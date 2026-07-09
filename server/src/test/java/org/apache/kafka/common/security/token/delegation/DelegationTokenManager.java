/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.token.delegation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/** Test-only token provider used when Kafka is not installed on the classpath. */
public final class DelegationTokenManager {

  private DelegationTokenManager() {
  }

  public static void obtainDelegationTokens(
      String proxyUser,
      String renewer,
      Configuration conf,
      Credentials creds) {
    Token<TokenIdentifier> token = new Token<>();
    creds.addToken(new Text("KAFKA_DELEGATION_TOKEN"), token);
  }
}
