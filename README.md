# Data Sharing Service

## Overview

The data sharing service is what makes data available both to plugins on the node and to beehive.

## Design

This service's primary task is to:

* Handle RabbitMQ messages published by plugins.
* Validate and add app metadata to messages.
* Republish messages local and to beehive.

## Running the Test Suite

The test suite can be run using:

```sh
make svc-up

# wait a moment for everything to start...

make test
```
