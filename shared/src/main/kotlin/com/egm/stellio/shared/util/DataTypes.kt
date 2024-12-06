package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SYSATTRS_TERMS
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.http.MediaType

// regroups utils function specific to data types defined in 5.2
object DataTypes {

    // unknown properties are not allowed (in contrast to entities)
    val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    inline fun <reified T> deserializeAs(content: String): T =
        mapper.readValue(content, T::class.java)

    fun toFinalRepresentation(
        dataType: Map<String, Any>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): Map<String, Any> =
        this.let {
            if (mediaType == MediaType.APPLICATION_JSON)
                dataType.minus(JSONLD_CONTEXT)
            else dataType
        }.let {
            if (!includeSysAttrs)
                dataType.minus(NGSILD_SYSATTRS_TERMS)
            else dataType
        }
}
