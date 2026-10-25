Look up the NGSI-LD spec (ETSI TS 104 175 Core API, TS 104 176 HTTP Binding) by section number or keyword, then run a compliance check if implementation code
is provided.

Usage examples:
- `/project:spec 10.2.4` — retrieve section 10.2.4 (from both documents, as numbers overlap)
- `/project:spec http 7.4.3.1` — retrieve section 7.4.3.1 of TS 104 176 only (`core` for TS 104 175)
- `/project:spec "batch operations"` — keyword search
- `/project:spec http 7.2 EntityHandler.kt` — retrieve section + audit the named file

## Steps

1. Parse $ARGUMENTS:
   - If it looks like a section number (digits and dots, e.g. `10.2.4`): call `get_spec_section` via the `ngsi-ld-spec` MCP,
     passing `document` (`core` or `http`) if given.
   - If it is a phrase or keyword: call `search_spec` via the `ngsi-ld-spec` MCP (same `document` rule).
   - If both a section/keyword and a file path are given: retrieve the spec text first, then read the file.

2. Display the retrieved spec text verbatim, clearly labelled with the document and section number.

3. If a file was provided (or can be inferred from context), invoke the `stellio-spec-checker` agent to audit the
implementation against the retrieved spec section. The checker will report deviations in priority order per its instructions.

4. If deviations are found, state: "Hand off to `stellio-kotlin-dev` to implement fixes." and list the deviations as a
numbered action list.
