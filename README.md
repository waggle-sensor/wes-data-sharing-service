# Data Sharing Service

## Overview

The data sharing service is what makes data available both to plugins on the node and to beehive.

## Design

```txt
┌─────────┐
│┌─────────┐       ┌──────────┐  Scope Fork   ┌───────────┐
└│┌─────────┐ ───> │ Validate │ ──────┬─────> │ Serialize │ ───> To Beehive
 └│ Plugins │      │ Message  │       v       │ to Waggle │
  └─────────┘      └──────────┘   To Plugins  └───────────┘
                        ^
                        |  Map plugin Pod
                        | runtime metadata.
                        v
                  ┌───────────┐
                  │  k3s API  |
                  └───────────┘
```

* The "validate message" stage checks to see if the message is a valid intra-node message. Technical note: We _do not_ check if the name exists in the ontology here. Two very interesting use cases were pointed out:
  * If I am in early development, I probably want to start trying out data sharing without being blocked by having to predefine everything.
  * If I am building a set of closely related apps, I may want local only messages which don't need to be in the ontology.
* The "serialize to waggle" stage takes the intra-node message format and uses the ontology / SDF / PDF to serialize it to a waggle protocol message.
