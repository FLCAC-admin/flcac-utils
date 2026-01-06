# Format specifications for parameter data
- Structure of olca parameter dictionaries stored in 'parameters' list within each olca process dictionary
- Input parameters are independent variables; dependent parameters are evaluated formulas using input parameters

| Field            | Type  | Required |  Note |
|:-----------------|:-----:|:--------:|:------|
| ProcessName      | str   | Y        | Name of the process that the parameter is associated with |
| formula          | str   | N        | Mathematical formula for dependent parameters; null (nan) for independent parameters |
| isInputParameter | bool  | Y        | True for input parameters; False for dependent parameters |
| name             | str   | Y        | Name of the input or dependent parameter |
| value            | float | N        | Numerical value for input parameters; null (nan) for dependent parameters |
| description      | str   | N        | Description of input and dependent parameters|
