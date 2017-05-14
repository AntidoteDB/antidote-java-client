# Installation

The library is not yet available in public Maven repositories, so you have to install it locally:

	git clone https://github.com/SyncFree/antidote-java-client.git
	cd antidote-java-client
	./gradlew install

After installation you can add the dependency to your project:


## Maven

	<dependency>
		<groupId>eu.antidotedb</groupId>
		<artifactId>antidote-java-client</artifactId>
		<version>0.0.1</version>
	</dependency>

## Gradle


	compile group: 'eu.antidotedb', name: 'antidote-java-client', version: '0.0.1'


# Usage

First add the necessary imports if you are not using an IDE that adds them automatically:

    import eu.antidotedb.client.*;

To connect to Antidote, create a new `AntidoteClient` instance and pass it one or more `Host` entries:

    AntidoteClient antidote = new AntidoteClient(new Host("localhost", 8087));

You can also pass a `PoolManager` to get more control over the configuration.

## Antidote-Objects

In Antidote each object is stored in a bucket.
To create a bucket use the static `Bucket.create` method:

    Bucket<String> bucket = Bucket.create("mybucket");

The type parameter of bucket denotes the type of keys used to address objects stored in the bucket.
The default is `String`, but you can pass a `ValueCoder` and choose a different type for the key (and be more typesafe).

Objects in the database are addressed using immutable references of type `ObjectRef`, which can be created using methods on the `Bucket` object.
Each datatype supported by Antidote has its own method.
For example a reference to a set datatype stored under key "users" can be retrieved as follows:

    SetRef<String> userSet = bucket.set("users");

The type parameter of `SetRef` denotes the type of elements stored in the set.
As with keys, this can be configured by passing a `ValueCoder` to the `set` method.

A list of available types can be found in the `CrdtContainer` interface, which is implemented by the `Bucket` class.


## Reading objects

Each `ObjectRef` has a `read` method, which retrieves the current value of the object from the database.
The `read` method takes a transaction object.
Use the `AntidoteClient.noTransaction` method to execute the request without any transactional context:

    List<String> value = userSet.read(antidote.noTransaction());


For reading multiple objects simultaneously, a `BatchRead` can be used.
Start a batch read with the `AntidoteClient.newBatchRead` method.
Then pass the result to several read-requests.
Finally, the results can be obtained via the `BatchReadResult.get` methods:

    CounterRef c1 = bucket.counter("c1");
    CounterRef c2 = bucket.counter("c1");
    CounterRef c3 = bucket.counter("c1");
    BatchRead batchRead = antidote.newBatchRead();
    BatchReadResult<Integer> c1val = c1.read(batchRead);
    BatchReadResult<Integer> c2val = c2.read(batchRead);
    BatchReadResult<Integer> c3val = c3.read(batchRead);
    batchRead.commit();
    int sum = c1val.get() + c2val.get() + c3val.get();

## Updating objects

Each `ObjectRef` has one or more methods to perform updates.
Just like with reads, updates take a transactional context as their first parameters.

For example a `SetRef` has an `add` method to add elements to the set:

    bucket.set("users").add(antidote.noTransaction(), "Hans Wurst");


## ValueCoder

TODO



## Session guarantees

To ensure session guarantees like "read your writes" Antidote uses vector clocks.
Each operation returns a vector clock indicating the time after the operation.
At each request to Antidote a vector clock can be given to force a minimum time for the snapshot used in the request.

TODO



## Transactions


A transaction can be started with the `startTransaction` method on the `AntidoteClient` object.
It is good practice to use the resulting `InteractiveTransaction` with a try-with-resource statement to avoid leaving transactions open accidentially.

    try (InteractiveTransaction tx = antidote.startTransaction()) {
        int i = bucket.counter("my_counter").read(tx);
        bucket.set("my_set").add(tx, "i:" + i);
        tx.commitTransaction();
    }

## Mutable API

Call `toMutable` on a concrete `ObjectRef` to get a mutable view of the datatype.
This view can be updated with the `pull` method and local updates to the mutable updates can be sent to the database with the `push` method.

Example:

    MapRef<String> testmapRef = bucket.map_aw("testmap2");
    CrdtMap<String, CrdtSet<String>> testmap = testmapRef.toMutable(CrdtSet.creator(ValueCoder.utf8String));

    CrdtSet<String> a = testmap.get("a");
    a.add("1");
    a.add("2");
    CrdtSet<String> b = testmap.get("b");
    b.add("3");
    testmap.push(antidoteClient.noTransaction());


Warning: The mutable objects are **not** thread-safe.

<!--
# Configuration File

Antidote Java Client requires a configuration file whoch provides information about the Hostname
and Port number of the database to connect to.

A default configuration file can be generated using the `AntidoteConfigManager.generateDefaultConfig()` method.
the default configuration file (config.xml) would be generated in the `user.dir`.

You can also specify the configuration file path using the methods of `AntidoteConfigManager` class.

# Usage

To connect to Antidote, you need to instantiate an object of `AntidoteClient` class. To get an object of AntidoteClient
class, you need to get a `PoolManager` which handles the connection to the Antidote Database Engine.

Sample usage of instantiating connection to Antidote:
~~~~
String configFilePath = "config.xml"
antidotePoolManager = new PoolManager(20, 5, configFilePath);
antidoteClient = new AntidoteClient(antidotePoolManager);
~~~~

## AntidoteClient

Objects in the database are addressed using immutable references of type [[AntidoteObject]], which can be retrieved using
methods on the AntidoteClient object.
Each datatype supported by Antidote has its own method.
For example a reference to a set datatype stored under key "users" in a specified bucket can be retrieved as follows:

    RWSetRef  userSetRef = antidoteClient.rwSetRef("users", bucket);
    AntidoteOuterRWSet userSetRef.createAntidoteRWSet();

## Reading Objects

Depending on the object type, every object has a getValue(), getValues(), or getValueList() method to fetch its values.
The result is returned as a String or a Set of Strings.

    userSetRef.readDatabase();
    Set<String> setValues = userSetRef.getValues();

## Updating Objects

Objects are updated by creating an AntidoteTransaction object which is then passed as a parameter to the update method of
the specified object.

    AntidoteTransaction tx = antidoteClient.createStaticTransaction();
    userSet.addElement("UserTest", tx);
    tx.commitTransaction();
    tx.close();

-->