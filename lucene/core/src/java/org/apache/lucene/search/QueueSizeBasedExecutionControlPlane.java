/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Derivative of SliceBasedExecutionControlPlane that controls the number of active threads
 * that are used for a single query. At any point, no more than (maximum pool size of the executor * LIMITING_FACTOR)
 * threads should be active. If the limit is exceeded, further segments are searched on the caller thread
 */
public class QueueSizeBasedExecutionControlPlane extends SliceExecutionControlPlane {
  private static final double LIMITING_FACTOR = 1.5;

  private final ThreadPoolExecutor threadPoolExecutor;

  public QueueSizeBasedExecutionControlPlane(ThreadPoolExecutor threadPoolExecutor) {
    super(threadPoolExecutor);
    this.threadPoolExecutor = threadPoolExecutor;
  }

  @Override
  public List<Future> invokeAll(Collection<FutureTask> tasks) {
    List<Future> futures = new ArrayList();
    int i = 0;

    for (FutureTask task : tasks) {
      boolean shouldExecuteOnCallerThread = false;

      // Execute last task on caller thread
      if (i == tasks.size() - 1) {
        shouldExecuteOnCallerThread = true;
      }

      if (threadPoolExecutor.getQueue().size() >=
          (threadPoolExecutor.getMaximumPoolSize() * LIMITING_FACTOR)) {
        shouldExecuteOnCallerThread = true;
      }

      processTask(task, futures, shouldExecuteOnCallerThread);

      ++i;
    }

    return futures;
  }
}
