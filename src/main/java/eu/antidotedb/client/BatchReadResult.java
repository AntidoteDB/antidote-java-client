package eu.antidotedb.client;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 */
public interface BatchReadResult<T> {
    /**
     * Get the result from the batch-read.
     * <p>
     * Performs the batch read, if the value is not yet available
     */
    T get();

    void whenReady(Consumer<T> f);

    default <S> BatchReadResult<S> map(Function<T, S> f) {
        BatchReadResult<T> parent = this;
        return new BatchReadResult<S>() {
            S cache = null;

            @Override
            public S get() {
                if (cache == null) {
                    T t = parent.get();
                    cache = f.apply(t);
                }
                return cache;
            }

            @Override
            public void whenReady(Consumer<S> callback) {
                if (cache == null) {
                    parent.whenReady(t -> callback.accept(get()));
                } else {
                    callback.accept(cache);
                }
            }
        };
    }
}
