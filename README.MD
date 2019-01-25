
# Antidote Java Client

The repository contains classes for using Antidote Database service in Java. It provides the client implementation to
use Antidote Database.

You can learn more about Antidote Database [here](http://syncfree.github.io/antidote/)

## WARNING: Work in progress

The API will change and not be backwards compatible!

We will provide documentation and Maven artifacts once the library is ready to be used.

# Documentation

[![Javadocs](https://www.javadoc.io/badge/eu.antidotedb/antidote-java-client.svg)](https://www.javadoc.io/doc/eu.antidotedb/antidote-java-client)

Documentation is available via javadoc, browsable online via [JavaDoc.io](https://www.javadoc.io/doc/eu.antidotedb/antidote-java-client/).
The overview page contains a short tutorial.

# Development / Contributing

[![Build Status](https://travis-ci.org/AntidoteDB/antidote-java-client.svg?branch=master)](https://travis-ci.org/AntidoteDB/antidote-java-client)

The project requires Java 8 and uses gradle as a build tool.
To build the project run the gradle wrapper included in the project:

    ./gradlew build

The tests require an Antidote instance running locally on port 8087.
You can use Docker to start an instance:

    docker run --rm -it -p "8087:8087" mweber/antidotedb


Feel free to open pull requests or open issues.
