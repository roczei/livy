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

package org.apache.livy.rsc.driver;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.lang.reflect.Method;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies a refreshed {@link Credentials} blob on the currently running driver JVM.
 *
 * <p>The blob is expected to be in Hadoop's standard {@code Credentials} wire format
 * (as produced by {@link Credentials#writeTokenStorageToStream}). Applying it is a
 * two-step process:
 *
 * <ol>
 *   <li>{@code UserGroupInformation.getCurrentUser().addCredentials(creds)} — makes
 *       the new tokens visible to any Hadoop client running inside the driver JVM
 *       (HDFS, Hive, HBase, ...).</li>
 *   <li>Reflectively invokes {@code SparkEnv.get().schedulerBackend()
 *       .updateDelegationTokens(byte[])} — Spark itself then broadcasts an
 *       {@code UpdateDelegationTokens} message to all executors, so each executor
 *       JVM picks the new tokens up too.</li>
 * </ol>
 *
 * <p>Reflection is used because {@code CoarseGrainedSchedulerBackend
 * .updateDelegationTokens} has varied visibility across Spark versions; we do not
 * want to hard-depend on a particular Spark internal API here. When Spark or its
 * scheduler backend is not present (e.g. unit tests, non-YARN local mode without
 * a running SparkContext), the reflective step is skipped and only the local UGI
 * is updated.
 */
final class DelegationTokenUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(DelegationTokenUpdater.class);

  private DelegationTokenUpdater() {}

  static void applyCredentials(byte[] credentialsBytes) throws Exception {
    Credentials creds = new Credentials();
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(credentialsBytes))) {
      creds.readTokenStorageStream(in);
    }
    UserGroupInformation.getCurrentUser().addCredentials(creds);
    LOG.info("Installed {} refreshed delegation tokens on the driver UGI.",
        creds.numberOfTokens());
    propagateToSpark(credentialsBytes);
  }

  /**
   * Best-effort reflective call into Spark's SchedulerBackend to broadcast the new
   * tokens to executors. Any exception is logged at WARN and swallowed — the driver
   * JVM already has the fresh tokens installed on its own UGI, which is the primary
   * requirement.
   */
  private static void propagateToSpark(byte[] credentialsBytes) {
    try {
      Class<?> sparkEnv = Class.forName("org.apache.spark.SparkEnv");
      Object env = sparkEnv.getMethod("get").invoke(null);
      if (env == null) {
        LOG.debug("SparkEnv is not initialized; skipping executor token broadcast.");
        return;
      }
      Object backend = sparkEnv.getMethod("schedulerBackend").invoke(env);
      if (backend == null) {
        LOG.debug("Spark SchedulerBackend is not available; " +
            "skipping executor token broadcast.");
        return;
      }
      // updateDelegationTokens(byte[]) is defined on CoarseGrainedSchedulerBackend and
      // its subclasses. If the current backend is a LocalSchedulerBackend or a mock,
      // the method may not exist — treat that as a no-op.
      Method update = findUpdateMethod(backend.getClass());
      if (update == null) {
        LOG.debug("Scheduler backend {} does not expose updateDelegationTokens; " +
            "skipping executor token broadcast.", backend.getClass().getName());
        return;
      }
      update.setAccessible(true);
      update.invoke(backend, (Object) credentialsBytes);
      LOG.info("Broadcasted refreshed delegation tokens to Spark executors.");
    } catch (ClassNotFoundException e) {
      LOG.debug("Spark not on the classpath; skipping executor token broadcast.");
    } catch (Exception e) {
      LOG.warn("Failed to broadcast refreshed delegation tokens to Spark executors " +
          "(driver-side UGI is still refreshed).", e);
    }
  }

  private static Method findUpdateMethod(Class<?> clazz) {
    Class<?> c = clazz;
    while (c != null && c != Object.class) {
      try {
        return c.getDeclaredMethod("updateDelegationTokens", byte[].class);
      } catch (NoSuchMethodException e) {
        c = c.getSuperclass();
      }
    }
    return null;
  }
}
