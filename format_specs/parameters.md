# Format specifications for parameter data

- **Process-scoped** parameters are stored in the `parameters` list on each olca process.
- **Global** parameters are standalone olca `Parameter` entities with
  `parameterScope=GLOBAL_SCOPE`, written as top-level JSON-LD objects (via
  `build_global_param_dict` + `write_objects`).
- Input parameters are independent variables; dependent parameters are evaluated
  formulas using input parameters (process and/or global).

| Field            | Type  | Required | Note                                                                   |
|:-----------------|:-----:|:--------:|:-----------------------------------------------------------------------|
| processName      | str   | N*       | Required for `PROCESS_SCOPE`; omit/empty for `GLOBAL_SCOPE`            |
| formula          | str   | N        | Formula for dependent parameters; null for input parameters            |
| isInputParameter | bool  | Y        | True for input parameters; False for dependent parameters              |
| name             | str   | Y        | Name of the input or dependent parameter                               |
| value            | float | N        | Value for input parameters; null for dependent parameters              |
| description      | str   | N        | Description of the parameter                                           |
| parameterScope   | str   | N        | `PROCESS_SCOPE` (default) or `GLOBAL_SCOPE`                            |
