Run tests and open the Jacoco coverage report for a module.

## Steps

1. Determine the target module from $ARGUMENTS. If not provided, infer it from the current working directory or recently edited files. If still ambiguous, ask.

2. Run the module's tests:
   ```bash
   ./gradlew :<module>:test
   ```
   Wait for completion. Report pass/fail counts.

3. Open the HTML coverage report:
   ```bash
   open <module>/build/reports/jacoco/test/html/index.html
   ```

4. Report the overall line coverage percentage from the report summary. If coverage is below 75%, list the top 5 classes with the lowest coverage so the user knows where to focus.

Supported modules: `search-service`, `subscription-service`, `shared`, `api-gateway`
