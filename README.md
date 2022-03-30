# Data Sharing Service

## Overview

The data sharing service is what makes data available both to plugins on the node and to beehive.

## Design

This service's primary task is to:

1. Handle RabbitMQ messages published by plugins.
2. Handle Kubernetes plugin pod events and cache pod metadata for tagging.
3. Validate and add pod metadata to messages.
4. Republish messages local and to beehive.

This service is designed to have the following behavior:

1. Valid messages with known pod metadata are immediately published.
2. Valid messages with unknown pod metadata are added to a backlog for the message's pod UID.
3. When a pod event is handled, the backlog for that Pod UID is immediately flushed.
4. Pod metadata expires after config.pod_state_expire_duration seconds. Any pod or
    message events reset the expiration time for the message's pod UID. When a pod
    expires all messages in the backlog are dropped.
