package com.egm.stellio.search.service.util

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.ErrorMessages.ServiceExecution.SERVICE_INPUT_SCHEMA_MISMATCH_MESSAGE
import com.egm.stellio.shared.util.ErrorMessages.ServiceRegistration.invalidServiceSchemaMessage
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.networknt.schema.InputFormat
import com.networknt.schema.Schema
import com.networknt.schema.SchemaLocation
import com.networknt.schema.SchemaRegistry
import com.networknt.schema.SpecificationVersion
import tools.jackson.databind.JsonNode

object ServiceSchemaValidator {
    // Shared configuration and meta-schema cache; inline schemas get their own schema context.
    private val registry: SchemaRegistry =
        SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12) { builder ->
            builder.schemaLoader { loader ->
                // block usage of "$ref" to only the local schemas
                loader.fetchRemoteResources(false).block { it.toString().startsWith("classpath:") }
            }
        }

    private fun compile(schema: JsonNode, member: String): Either<APIException, Schema> =
        Either.catch {
            val compiled = registry.getSchema(schema.toString(), InputFormat.JSON)
            val dialect = compiled.schemaContext.dialect.id
            val errors = registry.getSchema(SchemaLocation.of(dialect)).validate(schema)
            require(errors.isEmpty()) { errors.joinToString("; ") { it.message } }
            compiled.apply { initializeValidators() }
        }.mapLeft { BadRequestDataException(invalidServiceSchemaMessage(member), detail = it.message) }

    fun validateSchema(schema: JsonNode, member: String): Either<APIException, Unit> =
        compile(schema, member).map { }

    fun validateInput(schema: JsonNode, input: Any): Either<APIException, Unit> = either {
        val compiled = compile(schema, "input").bind()
        val errors = Either.catch { compiled.validate(serializeObject(input), InputFormat.JSON) }
            .mapLeft { BadRequestDataException(SERVICE_INPUT_SCHEMA_MISMATCH_MESSAGE, detail = it.message) }.bind()
        ensure(errors.isEmpty()) {
            BadRequestDataException(
                SERVICE_INPUT_SCHEMA_MISMATCH_MESSAGE,
                detail = errors.joinToString("; ") { "${it.instanceLocation}: ${it.message}" }
            )
        }
    }
}
