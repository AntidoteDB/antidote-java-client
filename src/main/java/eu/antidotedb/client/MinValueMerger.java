package eu.antidotedb.client;

import java.util.Collections;
import java.util.List;

public class MinValueMerger<V extends Comparable<V>> implements MergeRegisterKey.ValueMerger<V> {
    private V onEmpty;

    public MinValueMerger(V onEmpty) {
        this.onEmpty = onEmpty;
    }

    @Override
    public V merge(List<V> concValues) {
        if (concValues.isEmpty()) {
            return onEmpty;
        } else {
            return Collections.min(concValues);
        }
    }
}
