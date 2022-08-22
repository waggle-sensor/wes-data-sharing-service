# Data Sharing Service

## Overview

The data sharing service is what makes data available both to plugins on the node and to beehive.

## Design

This service's primary task is to:

* Handle RabbitMQ messages published by plugins.
* Validate and add app metadata to messages.
* Republish messages local and to beehive.

This service fits into the app subsystem as follows:

```
           app init container
        1. sets app meta cache:
           app uid -> app meta

  (app) ------------------> [app meta cache]
    |                                |
    | 2. publish to                  |
    |    to-validator                |
    |    with app uid                | 3. get and tag message meta using app uid
    |                                |
    +-> [|||||] ----------> [data sharing service] ---> 4. publish to local / beehive
  to-validator
  queue
```

1. When an app is deployed, an init container sets `app-meta-cache[app uid] = app meta` before running the main container. (And upon restarts / faiures / reassignments of the Pod.)
2. Apps publish messages to the `to-validator` queue tagged with the app UID.
3. Data sharing service processes the `to-validator` queue by tagging them with app meta data based on the app UID and then republishing locally and to beehive.
  * Messages without an app UID are rejected.
  * Messages with invalid meta data are rejected.

Note: The [app meta cache](https://github.com/waggle-sensor/waggle-edge-stack/tree/main/kubernetes/wes-app-meta-cache) is currently configured as a 10MB LRU cache. Pod meta data is generally less than 256 bytes, so this leaves us with a meta cache of the last ~10K apps.

## Running the Test Suite

The test suite can be run using:

```sh
make svc-up

# wait a moment for everything to start...

make test
```
