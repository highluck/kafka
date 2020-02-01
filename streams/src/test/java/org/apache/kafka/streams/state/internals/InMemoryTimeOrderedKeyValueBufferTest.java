/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InMemoryTimeOrderedKeyValueBufferTest {

    @Test
    public void bufferShouldAllowCacheEnablement() {
        new InMemoryTimeOrderedKeyValueBuffer.Builder<>(null, null, null)
            .withCachingEnabled();
    }

    @Test
    public void bufferShouldAllowCacheDisablement() {
        new InMemoryTimeOrderedKeyValueBuffer.Builder<>(null, null, null)
            .withCachingDisabled();
    }

    @Test
    public void bufferShouldAllowLoggingEnablement() {
        final String expect = "3";
        final Map<String, String> logConfig = new HashMap<>();
        logConfig.put("min.insync.replicas", expect);
        final StoreBuilder builder = new InMemoryTimeOrderedKeyValueBuffer.Builder<>(null, null, null)
            .withLoggingEnabled(logConfig);

        assertTrue(builder.logConfig().containsKey("min.insync.replicas"));
        assertTrue(builder.logConfig().containsValue(expect));
    }

    @Test
    public void bufferShouldAllowLoggingDisablement() {
        final StoreBuilder builder = new InMemoryTimeOrderedKeyValueBuffer.Builder<>(null, null, null)
            .withLoggingDisabled();

        assertEquals(builder.logConfig(), new HashMap<>());
    }
}
