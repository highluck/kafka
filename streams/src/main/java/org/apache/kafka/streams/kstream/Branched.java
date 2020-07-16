package org.apache.kafka.streams.kstream;


import java.util.Objects;
import java.util.function.Consumer;

public class Branched<K, V> implements NamedOperation<Branched<K, V>> {
    protected String processorName;
    protected Consumer<? super KStream<? super K, ? super V>> consumer;

    public Branched(final String processorName,
                    final Consumer<? super KStream<? super K, ? super V>> consumer) {
        this.processorName = processorName;
        this.consumer = consumer;
    }

    public Branched(final Branched<K, V> branched) {
        this(branched.processorName, branched.consumer);
    }

    public static <K, V> Branched<K, V> as(final String name) {
        return new Branched<>(name, null);
    }

    public static <K, V> Branched<K, V> with(final String name,
                                             final Consumer<? super KStream<? super K, ? super V>> consumer) {
        return new Branched<>(name, consumer);
    }

    @Override
    public Branched<K, V> withName(final String name) {
        this.processorName = name;
        return this;
    }

    public Branched<K, V> withConsumer(final Consumer<? super KStream<? super K, ? super V>> consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Branched<?, ?> branched = (Branched<?, ?>) o;
        return Objects.equals(processorName, branched.processorName) &&
                Objects.equals(consumer, branched.consumer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processorName, consumer);
    }
//    public static Branched<K, V> as(final String name) {
//
//    }
//    static Branched<K, V> with(Consumer<? super KStream<? super K, ? super V>> chain);
}
