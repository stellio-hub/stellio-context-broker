Look up ETSI GS CIM 009 (NGSI-LD spec) by section number or keyword, then run a compliance check if implementation code
is provided.

Usage examples:
- `/project:spec 5.6.3` — retrieve section 5.6.3
- `/project:spec "batch operations"` — keyword search
- `/project:spec 5.7 EntityHandler.kt` — retrieve section + audit the named file

## Steps

1. Parse $ARGUMENTS:
   - If it looks like a section number (digits and dots, e.g. `5.6.3`): call `get_spec_section` via the `ngsi-ld-spec` MCP.
   - If it is a phrase or keyword: call `search_spec` via the `ngsi-ld-spec` MCP.
   - If both a section/keyword and a file path are given: retrieve the spec text first, then read the file.

2. Display the retrieved spec text verbatim, clearly labelled with the section number.

3. If a file was provided (or can be inferred from context), invoke the `stellio-spec-checker` agent to audit the
implementation against the retrieved spec section. The checker will report deviations in priority order per its instructions.

4. If deviations are found, state: "Hand off to `stellio-kotlin-dev` to implement fixes." and list the deviations as a
numbered action list.
