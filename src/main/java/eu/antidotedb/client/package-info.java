/**
 * This is the main package of the Antidote Java Client.
 * <h1>Usage</h1>
 * <p>First add the necessary imports if you are not using an IDE that adds them automatically:</p>
 * <pre><code>
 *     import eu.antidotedb.client.*;
 *     import java.net.InetSocketAddress;
 * </code></pre>
 * <p>To connect to Antidote, create a new {@link eu.antidotedb.client.AntidoteClient} instance and pass it one or more
 * {@link java.net.InetSocketAddress}
 * entries:</p>
 * <pre><code>
 *     AntidoteClient antidote = new AntidoteClient(new InetSocketAddress("localhost", 8087));
 * </code></pre>
 * <p>You can also pass a {@link eu.antidotedb.client.PoolManager} to get more control over the configuration.</p>
 * <h2>Antidote-Keys</h2>
 * <p>Objects in the database are addressed using a {@link eu.antidotedb.client.Key}.
 * A key contains the CRDT type and a name of type ByteString.
 * To create a key the static methods on {@link eu.antidotedb.client.Key} can be used:
 * <ul>
 * <li>{@link eu.antidotedb.client.Key#register register}
 * <li>{@link eu.antidotedb.client.Key#multiValueRegister multiValueRegister}
 * <li>{@link eu.antidotedb.client.Key#counter counter}
 * <li>{@link eu.antidotedb.client.Key#fatCounter fatCounter}
 * <li>{@link eu.antidotedb.client.Key#map_g map_g}
 * <li>{@link eu.antidotedb.client.Key#map_rr map_rr}
 * <li>{@link eu.antidotedb.client.Key#set_removeWins set_removeWins}
 * <li>{@link eu.antidotedb.client.Key#set set}
 * <li>{@link eu.antidotedb.client.Key#flag_ew flag_ew}
 * <li>{@link eu.antidotedb.client.Key#flag_dw flag_dw}
 * <p>
 * </li>
 * </ul>
 * <p>For example a key for a set datatype stored under key "users" can be retrieved as follows:</p>
 * <pre><code>
 *     SetKey&lt;String&gt; userSet = Key.set("users");
 * </code></pre>
 * <p>
 * It is also possible to statically import the methods for creating keys:
 * <pre><code>
 *     import static eu.antidotedb.client.Key.*;
 *     ...
 *     SetKey&lt;String&gt; userSet = set("users");
 * </code></pre>
 * <p>The type parameter of {@link eu.antidotedb.client.SetKey SetKey} denotes the type of elements stored in the set.
 * The default is String, but this can easily be configured by passing a {@link eu.antidotedb.client.ValueCoder} to the {@link
 * eu.antidotedb.client.Key#set set} method.
 * For example a key for a set of ByteStrings can be created like this:</p>
 * <pre>
 *     SetKey&lt;ByteString&gt; userSetB = set("users", ValueCoder.bytestringEncoder);
 * </pre>
 * <p>
 * More information is available in the documentation of the {@link eu.antidotedb.client.ValueCoder ValueCoder} interface.
 * <h2>Buckets</h2>
 * <p>In Antidote each object is stored in a {@link eu.antidotedb.client.Bucket}.
 * To create a bucket use the static {@link eu.antidotedb.client.Bucket#bucket bucket} method:</p>
 * <pre><code>
 *     Bucket bucket = Bucket.bucket("mybucket");
 * </code></pre>
 * <h2>Reading objects</h2>
 * <p>A {@link eu.antidotedb.client.Bucket} has a {@link eu.antidotedb.client.Bucket#read read} method, which
 * retrieves the current value of the
 * object from the database.
 * The {@link eu.antidotedb.client.Bucket#read read} method takes a transaction object.
 * Use the {@link eu.antidotedb.client.AntidoteClient#noTransaction()} method to execute the request without any transactional context:
 * </p>
 * <pre><code>List&lt;String&gt; value = bucket.read(antidote.noTransaction(), userSet);
 * </code></pre>
 * <p>For reading multiple objects simultaneously, a {@link eu.antidotedb.client.BatchRead} can be used.
 * Start a batch read with the {@link eu.antidotedb.client.AntidoteClient#newBatchRead()} method.
 * Then use the {@link eu.antidotedb.client.BatchRead} as the transaction context for several read-requests.
 * Finally, after committing the {@link eu.antidotedb.client.BatchRead} via the {@link eu.antidotedb.client.BatchRead#commit commit} method,
 * the results can be obtained via the {@link eu.antidotedb.client.BatchReadResult#get()} methods:</p>
 * <pre><code>
 *     CounterKey c1 = Key.counter("c1");
 *     CounterKey c2 = Key.counter("c2");
 *     CounterKey c3 = Key.counter("c3");
 *     BatchRead batchRead = antidoteClient.newBatchRead();
 *     BatchReadResult&lt;Integer&gt; c1val = bucket.read(batchRead, c1);
 *     BatchReadResult&lt;Integer&gt; c2val = bucket.read(batchRead, c2);
 *     BatchReadResult&lt;Integer&gt; c3val = bucket.read(batchRead, c3);
 *     batchRead.commit(antidoteClient.noTransaction());
 *     int sum = c1val.get() + c2val.get() + c3val.get();
 * </code></pre>
 * <p>
 * If all read values have the same time the {@link eu.antidotedb.client.Bucket#readAll Bucket.readAll}
 * method provides a shortcut for performing several reads simultaneously:
 * <pre><code>
 *     List&lt;Integer&gt; values = bucket.readAll(antidoteClient.noTransaction(), Arrays.asList(c1, c2, c3));
 * </code></pre>
 * <h2>Updating objects</h2>
 * <p>Each {@link eu.antidotedb.client.Key} has one or more methods to create update operations on the key.
 * To execute the update operation on a bucket it has to be passed to the {@link eu.antidotedb.client.Bucket#update Bucket.update} method.
 * Just like with reads, the update method take a transactional context as its first argument.</p>
 * <p>For example a {@link eu.antidotedb.client.SetKey} has an {@link eu.antidotedb.client.SetKey#add add} method to add
 * elements to the set:</p>
 * <pre><code>
 *     bucket.update(antidote.noTransaction(), set("users").add("Hans Wurst"));
 * </code></pre>
 * <p>
 * For performing several updates simultaneously the {@link eu.antidotedb.client.Bucket#updates Bucket.updates} methods can be used.
 * <p>
 * When constructing map updates, updates for nested CRDTs can be created in the same way as other updates, which makes
 * it possible to buid complex map-updates updating several components of the map at once:
 * <pre><code>
 *     MapKey testmap = map_aw("testmap2");
 *
 *     AntidoteStaticTransaction tx = antidoteClient.createStaticTransaction();
 *     bucket.update(tx,
 *         testmap.update(
 *             counter("a").increment(5),
 *             register("b").assign("Hello")
 *     ));
 * </code></pre>
 * <h2>Transactions</h2>
 * <p>A transaction can be started with the {@link eu.antidotedb.client.AntidoteClient#startTransaction startTransaction}
 * method on the {@link
 * eu.antidotedb.client.AntidoteClient} object.
 * It is good practice to use the resulting {@link eu.antidotedb.client.InteractiveTransaction} with a
 * try-with-resource statement to avoid leaving
 * transactions open
 * accidentially.</p>
 * <pre><code>
 *     ValueCoder&lt;Integer&gt; intCoder = ValueCoder.stringCoder(Object::toString, Integer::valueOf);
 *     CounterKey c = Key.counter("my_example_counter");
 *     SetKey&lt;Integer&gt; numberSet = Key.set("set_of_numbers", intCoder);
 *     try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
 *         int val = bucket.read(tx, c);
 *         bucket.update(tx, numberSet.add(val));
 *     }
 * </code></pre>
 * <h2>Session guarantees</h2>
 * <p>To ensure session guarantees like "read your writes" Antidote uses vector clocks.
 * Each operation returns a vector clock indicating the time after the operation.
 * At each request to Antidote a vector clock can be given to force a minimum time for the snapshot used in the
 * request.</p>
 * <p>TODO this is not yet implemented on the client
 */
package eu.antidotedb.client;