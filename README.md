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
```

* The "validate message" stage checks to see if messages exist in the ontology and if they have the expcted value type.

* The "serialize to waggle" stage takes the intra-node message format and uses the ontology / SDF / PDF to serialize it to a waggle protocol message.

## Message Format

The current intra-node message format is a JSON structure with fields:

* `ts`. Message timestamp (nanoseconds since epoch).
* `value`. Message value
            'ts': 0,
            'value': 22.9,
            'plugin': 'simple:0.1.0',
            'topic': 'env.temperature.tmp112',
        }