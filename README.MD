## WARNING: Work in progress

The API will change and not be backwards compatible!

We will provide documentation and Maven artifacts once the library is ready to be used.


# Antidote Java Client

The repository contains classes for using Antidote Database service in Java. It provides the client implementation to
use Antidote Database.

You can learn more about Antidote Database [here](http://syncfree.github.io/antidote/)


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

