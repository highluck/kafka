package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.kstream.internals.BranchedInternal;

public interface BranchedKStream<K, V> {
    BranchedKStream<K, V> branch(final Predicate<? super K, ? super V> predicate, final BranchedInternal<K, V> branched);
    BranchedKStream<K, V> defaultBranch(final BranchedInternal<K, V> branched);
}
