# Service execution draft remarks

- addition of `endpointMethod` to let the user define what method his endpoint use.
- Service information `input` and `output` contain JSON Schema documents, preserved as JSON.
- Execution input is validated with NetworkNT before persistence and invocation; output validation is not implemented.
- The document used `completed` but also `success` and `failure` i choose `success` and `failure` model.
- Calls to the Service Executor include the execution ID in the `Service-Execution` header; the body contains only the input.
- The base Service execution CRUD identifies executions by their resource URI, without requiring this header.
- added `progress` in the ServiceExecution who is `null` on creation and can be set through executor `PATCH` to a proportion from `0` to `1`.
- `DELETE` accepts `options=remove`, `options=cancel`, or `options=remove,cancel` and defaults to `cancel`; cancellation currently returns `NotImplemented`.
- patch only supports executionStatus, output, progress and responseStatusCode


#Thomas

- Can a synchronous execution be pending, executing, cancelled? (if someone abort the request?)
- What should the status be for asynchronous execution if the call returned an error?
  - for a success : executing with the response in the output
  - for an error : failure with the response in the output and the response status in `responseStatusCode` (deviation)

todo :
- query serviceExecutions?
- verification of the output?
- What should return a create serviceExecution if the service returned an error.
   - for now it returns a 201 created (because the service execution was created and the service was called)
   - the executionStatus become failure
   - the error response is stored in the output,
   - and the received status in `responseStatusCode`
   - for now the same behavior happens if we didn't managed to connect to the service with the gateway timeout statuscode and a generated problemDetails as output
- compaction/expansion of the serviceName in serviceRegistration
