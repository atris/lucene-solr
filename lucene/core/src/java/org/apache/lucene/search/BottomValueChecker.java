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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains the bottom value across multiple collectors
 */
abstract class BottomValueChecker<T> {
  /** Maintains global bottom score as the maximum of all bottom scores */
  private static class MaximumBottomScoreChecker extends BottomValueChecker<Float> {
    private final AtomicInteger globalBottomValue = new AtomicInteger();

    @Override
    public void updateThreadLocalBottomValue(Float value) {
      globalBottomValue.updateAndGet(currentValue -> Float.intBitsToFloat(currentValue) < value ? Float.floatToIntBits(value) : currentValue);
    }

    @Override
    public Float getBottomValue() {
      return Float.intBitsToFloat(globalBottomValue.getAcquire());
    }
  }

  public static BottomValueChecker createMaxBottomScoreChecker() {
    return new MaximumBottomScoreChecker();
  }

  public abstract void updateThreadLocalBottomValue(T value);
  public abstract T getBottomValue();
}
