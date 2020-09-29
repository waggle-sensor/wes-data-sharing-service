# Data Sharing Service

## Overview

The data sharing service is what makes data available both to plugins on the node and to beehive.

```txt
┌─────────┐
│┌─────────┐       ┌──────────┐  Scope Fork   ┌───────────┐
└│┌─────────┐ ───> │ Validate │ ──────┬─────> │ Serialize │ ───> To Beehive
 └│ Plugins │      │ Message  │       v       │ to Waggle │
  └─────────┘      └──────────┘   To Plugins  └───────────┘
```
