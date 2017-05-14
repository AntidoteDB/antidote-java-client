package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import com.google.protobuf.ByteString;

import java.util.*;

/**
 * The Class AntidoteOuterSet.
 */
public class CrdtSet<T> extends AntidoteCRDT implements Set<T> {


    private SetRef<T> ref;

    // current values
    private Set<T> values = new LinkedHashSet<T>();

    // uncommitted changes:
    private Set<T> added = new HashSet<T>();
    private Set<T> removed = new HashSet<T>();

    /**
     * Instantiates a new antidote set.
     */
    CrdtSet(SetRef<T> ref) {
        this.ref = ref;
    }


    @Override
    public SetRef<T> getRef() {
        return ref;
    }

    @Override
    public void updateFromReadResponse(AntidotePB.ApbReadObjectResp resp) {
        values.clear();
        added.clear();
        removed.clear();
        for (ByteString bytes : resp.getSet().getValueList()) {
            values.add(ref.getFormat().decode(bytes));
        }
    }

    @Override
    public void push(AntidoteTransaction tx) {
        ref.removeAll(tx, removed);
        ref.addAll(tx, added);
    }


    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return values.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        Iterator<T> iterator = values.iterator();
        return new Iterator<T>() {
            public T lastReturned;

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                lastReturned = iterator.next();
                return lastReturned;
            }

            @Override
            public void remove() {
                iterator.remove();
                added.remove(lastReturned);
                removed.add(lastReturned);
            }
        };
    }

    @Override
    public Object[] toArray() {
        return values.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        //noinspection SuspiciousToArrayCall
        return values.toArray(a);
    }

    @Override
    public boolean add(T t) {
        added.add(t);
        removed.remove(t);
        return values.add(t);
    }

    @Override
    public boolean remove(Object o) {
        added.remove(o);
        removed.add(getRef().getFormat().cast(o));
        return values.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return values.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        added.addAll(c);
        removed.removeAll(c);
        return values.addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Objects.requireNonNull(c);
        boolean modified = false;
        Iterator<T> it = iterator();
        while (it.hasNext()) {
            if (!c.contains(it.next())) {
                it.remove();
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        added.removeAll(c);
        for (Object o : c) {
            removed.add(getRef().getFormat().cast(o));
        }
        return values.removeAll(c);
    }

    @Override
    public void clear() {
        added.clear();
        removed.addAll(values);
        values.clear();
    }

    /**
     * returns a copy of the internal values in this set
     */
    public Set<T> getValues() {
        return new LinkedHashSet<>(values);
    }

    public static <V> CrdtCreator<CrdtSet<V>> creator(ValueCoder<V> valueCoder) {
        return new CrdtCreator<CrdtSet<V>>() {

            @Override
            public AntidotePB.CRDT_type type() {
                return AntidotePB.CRDT_type.ORSET;
            }

            @Override
            public <K> CrdtSet<V> create(CrdtContainer<K> c, K key) {
                return c.set(key, valueCoder).createAntidoteORSet();
            }

            @Override
            public CrdtSet<V> cast(AntidoteCRDT value) {
                //noinspection unchecked
                return (CrdtSet<V>) value;
            }
        };
    }

    public static <V> CrdtCreator<CrdtSet<V>> creatorRemoveWins(ValueCoder<V> valueCoder) {
        return new CrdtCreator<CrdtSet<V>>() {

            @Override
            public AntidotePB.CRDT_type type() {
                return AntidotePB.CRDT_type.RWSET;
            }

            @Override
            public <K> CrdtSet<V> create(CrdtContainer<K> c, K key) {
                return c.set_removeWins(key, valueCoder).createAntidoteRWSet();
            }

            @Override
            public CrdtSet<V> cast(AntidoteCRDT value) {
                //noinspection unchecked
                return (CrdtSet<V>) value;
            }
        };
    }
}
