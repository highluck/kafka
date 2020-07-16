package org.apache.kafka.streams.kstream.internals;


import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

import java.util.function.Consumer;

public class BranchedInternal<K, V> extends Branched<K, V> {

    public BranchedInternal(final String processorName,
                            final Consumer<? super KStream<? super K, ? super V>> consumer) {
        super(processorName, consumer);
    }

    public BranchedInternal(final Branched<K, V> branched) {
        super(branched);
    }

    public Consumer<? super KStream<? super K, ? super V>> consumer() {
        return consumer;
    }

    public String name() {
        return processorName;
    }
}
