# Format specifications for technosphere flow mapping file

| Field             | Type  | Required | Note                                                                                 |
|-------------------|-------|----------|--------------------------------------------------------------------------------------|
| SourceRepoName    |       | N        |                                                                                      |
| SourceFlowName    | str   | Y        | Flowname in the original dataset                                                     |
| SourceFlowUUID    |       | N        |                                                                                      |
| SourceFlowContext |       | N        |                                                                                      |
| SourceUnit        | str   | Y        | Unit in the original dataset                                                         |
| MatchCondition    |       |          |                                                                                      |
| ConversionFactor  | float | N        | Quantity to convert SourceUnit to TargetUnit, default is 1                           |
| TargetRepoName    | str   | Y        | Target repo name, if blank a new flow will be created                                |
| TargetFlowName    | str   | Y        | Target flow name, must match flow name exactly for existing flows                    |
| TargetUnit        | str   | Y        |                                                                                      |
| Provider          |       |          | Process Name of default provider if desired, if Bridge is `True`, this is not needed |
| Bridge            | bool  | N        | If `True`, will create an intermediary bridge process from BridgeFlowName to TargetFlowName        |
| BridgeFlowName    | str   | N        | Required if Bridge is `True`; new flow name                                                                                     |
| Mapper            |       | N        |                                                                                      |
| Verifier          |       | N        |                                                                                      |
| LastUpdated       |       | N        |                                                                                      |
| ConversionSource  |       | N        | If a conversion is needed, indicate the source for conversion factor                 |
