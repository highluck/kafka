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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class BranchedKStreamImpl<K, V> extends AbstractStream<K, V> implements BranchedKStream<K, V> {
    private static final String BRANDCH_NAME = "BRANCHED-KSTREAM-BRANCH-";

    private final boolean repartitionRequired;
    private final List<KStream<K, V>> branchChildren;

    BranchedKStreamImpl(final String name,
                        final Serde<K> keySerde,
                        final Serde<V> valueSerde,
                        final Set<String> subTopologySourceNodes,
                        final List<KStream<K, V>> branchChildren,
                        final boolean repartitionRequired,
                        final StreamsGraphNode streamsGraphNode,
                        final InternalStreamsBuilder builder) {
        super(name, keySerde, valueSerde, subTopologySourceNodes, streamsGraphNode, builder);
        this.repartitionRequired = repartitionRequired;
        this.branchChildren = branchChildren;
    }

    @Override
    public BranchedKStream<K, V> branch(final Predicate<? super K, ? super V> predicate,
                                        final BranchedInternal<K, V> branched) {
        return doBranch(predicate, branched);
    }

    @Override
    public BranchedKStream<K, V> defaultBranch(final BranchedInternal<K, V> branched) {
        return doBranch(null, branched);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private BranchedKStream<K, V> doBranch(final Predicate<? super K, ? super V> predicate,
                                           final BranchedInternal<K, V> branched) {
        Objects.requireNonNull(branched, "predicates can't be a null array");
        final NamedInternal namedInternal = new NamedInternal(branched.name());
        final String name = namedInternal.orElseGenerateWithPrefix(builder, BRANDCH_NAME);

        final StreamsGraphNode branchNode;
        if (predicate != null) {
            final ProcessorParameters<? super K, ? super V> parameters = new ProcessorParameters<>(
                    new KStreamLazyBranch<>(predicate, name),
                    name);
            branchNode = new ProcessorGraphNode<>(name, parameters);
            builder.addGraphNode(streamsGraphNode, branchNode);
        } else {
            branchNode = streamsGraphNode;
        }

        final ProcessorParameters innerProcessorParameters = new ProcessorParameters<>(new PassThrough<K, V>(), name);
        final ProcessorGraphNode<? super K, ? super V> branchChildNode = new ProcessorGraphNode<>(name, innerProcessorParameters);

        builder.addGraphNode(branchNode, branchChildNode);

        final KStream childStream = new KStreamImpl<>(name,
                keySerde,
                valueSerde,
                subTopologySourceNodes,
                repartitionRequired,
                branchChildNode,
                builder);

        branched.consumer().accept(childStream);
        return new BranchedKStreamImpl<>(name,
                keySerde,
                valueSerde,
                subTopologySourceNodes,
                branchChildren,
                repartitionRequired,
                streamsGraphNode,
                builder);
    }
}
