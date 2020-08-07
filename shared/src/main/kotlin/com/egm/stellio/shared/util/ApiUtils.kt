package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.http.MediaType
import java.time.ZonedDateTime
import java.time.format.DateTimeParseException
import java.util.Optional

object ApiUtils {

    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    fun addContextToParsedObject(parsedObject: Map<String, Any>, contexts: List<String>): Map<String, Any> {
        return parsedObject.plus(Pair("@context", contexts))
    }

    /**
     * As per 6.3.5, extract @context from request payload. In the absence of such context, then BadRequestDataException
     * shall be raised
     */
    fun getContextOrThrowError(input: String): List<String> {
        val rawParsedData = mapper.readTree(input) as ObjectNode
        val context = rawParsedData.get("@context") ?: throw BadRequestDataException("Context not provided")

        return try {
            mapper.readValue(
                context.toString(),
                mapper.typeFactory.constructCollectionType(List::class.java, String::class.java)
            )
        } catch (e: Exception) {
            throw BadRequestDataException(e.message ?: "Unable to parse the provided context")
        }
    }
}

fun String.parseTimeParameter(errorMsg: String): ZonedDateTime =
    try {
        ZonedDateTime.parse(this)
    } catch (e: DateTimeParseException) {
        throw BadRequestDataException(errorMsg)
    }

const val JSON_LD_CONTENT_TYPE = "application/ld+json"
const val JSON_MERGE_PATCH_CONTENT_TYPE = "application/merge-patch+json"
val JSON_LD_MEDIA_TYPE = MediaType.valueOf(JSON_LD_CONTENT_TYPE)

/**
 * As per 6.3.5, extract @context from Link header. In the absence of such Link header, it returns the default
 * JSON-LD @context.
 */
fun extractContextFromLinkHeader(linkHeader: List<String>): String {
    return if (linkHeader.isNotEmpty())
        linkHeader[0].split(";")[0].removePrefix("<").removeSuffix(">")
    else
        JsonLdUtils.NGSILD_CORE_CONTEXT
}

enum class OptionsParamValue(val value: String) {
    TEMPORAL_VALUES("temporalValues")
}

fun hasValueInOptionsParam(options: Optional<String>, optionValue: OptionsParamValue): Boolean =
    options
        .map { it.split(",") }
        .filter { it.any { option -> option == optionValue.value } }
        .isPresent
