# Format specifications for exchanges data

| Field           | Type  | Required |  Note |
|:----------------|:-----:|:--------:|:------|
| ProcessID       | str   | N        | ProcessID of the process, if not provided will be generated based on ProcessName |
| ProcessCategory | str   | Y        | Category (i.e., folder) of the process |
| ProcessName     | str   | Y        | Name of the process |
| FlowUUID        | str   | Y        | Unique hash for the flow in the exchange, can match the FEDEFL, an existing technosphere flow, or blank for a new flow |
| FlowName        | str   | Y        | Name of the flow |
| amountFormula   | str   | N        | Name of the parameter providing the exchange value |
| Context         | str   | N        | Required for` ELEMENTARY_FLOW`: FEDEFL context, e.g. `emission/air`; Required for NEW `PRODUCT_FLOW` or `WASTE_FLOW`: the category name; Not required for existing `PRODUCT_FLOW` or `WASTE_FLOW` |
| IsInput         | bool  | Y        | Inputs = `True`, Outputs = `False` |
| FlowType        | str   | Y        | `ELEMENTARY_FLOW`, `PRODUCT_FLOW`, or `WASTE_FLOW` |
| reference       | bool  | Y        | reference_flow = `True` |
| default_provider| str   | N        | UUID of the default provider for technosphere flows |
| amount          | float | Y        | Exchange amount |
| unit            | str   | Y        | Unit  |
| location        | str   | N        | process level location code, for countries this should be 2-digit code, e.g. `US` |
| avoided_product | bool  | N        | avoided_product = `True` |
| description     | str   | N        | description for the exchange |
| exchange_dqi    | str   | N        | semi-colon separated values for flow level dqi |
