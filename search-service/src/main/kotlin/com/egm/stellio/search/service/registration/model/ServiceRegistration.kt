package com.egm.stellio.search.service.registration.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.search.common.model.UnparsedGeoQuery
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.NGSILD_SERVICE_REGISTRATION_TERM
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.queryparameter.GeoQuery
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.queryparameter.parseQQuery
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.ErrorMessages.GenericValidation.invalidTypeMessage
import com.egm.stellio.shared.util.ErrorMessages.GenericValidation.invalidUriMessage
import com.egm.stellio.shared.util.ErrorMessages.GenericValidation.memberIsInvalidMessage
import com.egm.stellio.shared.util.ErrorMessages.ServiceRegistration.serviceRegistrationFailedToParseMessage
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import org.springframework.http.MediaType
import java.net.URI
import java.time.ZonedDateTime
import java.util.UUID

data class ServiceRegistration(
    val id: URI = "urn:ngsi-ld:ServiceRegistration:${UUID.randomUUID()}".toUri(),
    val type: String = NGSILD_SERVICE_REGISTRATION_TERM,
    val endpoint: URI,
    val entities: List<EntityInfo>,
    val serviceInformation: ServiceInformation,
    val q: String? = null,
    val geoQ: UnparsedGeoQuery? = null,
    val scopeQ: String? = null,
    val createdAt: ZonedDateTime = ngsiLdDateTime(),
    val modifiedAt: ZonedDateTime = createdAt,
) {
    fun expand(contexts: List<String>): ServiceRegistration =
        copy(
            entities = entities.map { it.expand(contexts) }
        )

    fun compact(contexts: List<String>): ServiceRegistration =
        copy(
            entities = entities.map { it.compact(contexts) }
        )

    fun serialize(
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE,
        includeSysAttrs: Boolean = false
    ): String =
        DataTypes.convertTo<Map<String, Any>>(compact(contexts))
            .plus(JSONLD_CONTEXT_KW to contexts)
            .let { DataTypes.toFinalRepresentation(it, mediaType, includeSysAttrs) }
            .let { DataTypes.serialize(it) }

    fun validate(): Either<APIException, Unit> = either {
        checkType().bind()
        checkUris().bind()
        checkEntities().bind()
        serviceInformation.validate().bind()
        checkQ().bind()
        checkGeoQ().bind()
    }

    fun mergeWithFragment(
        fragment: Map<String, Any>,
        contexts: List<String>
    ): Either<APIException, ServiceRegistration> = either {
        val mergedRegistration = DataTypes.convertTo<Map<String, Any>>(this@ServiceRegistration).plus(fragment)
        deserialize(mergedRegistration, contexts).bind()
            .copy(modifiedAt = ngsiLdDateTime())
    }

    private fun checkType(): Either<APIException, Unit> =
        if (type == NGSILD_SERVICE_REGISTRATION_TERM)
            Unit.right()
        else
            BadRequestDataException(invalidTypeMessage(NGSILD_SERVICE_REGISTRATION_TERM)).left()

    private fun checkUris(): Either<APIException, Unit> =
        when {
            !id.isAbsolute ->
                BadRequestDataException(invalidUriMessage(id.toString())).left()

            !endpoint.isAbsolute ->
                BadRequestDataException(invalidUriMessage(endpoint.toString())).left()

            entities.any { it.id?.isAbsolute == false } ->
                BadRequestDataException(
                    invalidUriMessage(entities.first { it.id?.isAbsolute == false }.id.toString())
                ).left()

            else -> Unit.right()
        }

    private fun checkEntities(): Either<APIException, Unit> = either {
        if (entities.isEmpty() || entities.any { it.types.isEmpty() || it.types.any(String::isBlank) }) {
            BadRequestDataException(memberIsInvalidMessage("entities"))
                .left()
                .bind<Unit>()
        }
        entities.forEach { it.validate().bind() }
    }

    private fun checkQ(): Either<APIException, Unit> =
        q?.let { parseQQuery(it).map { } } ?: Unit.right()

    private fun checkGeoQ(): Either<APIException, Unit> =
        geoQ?.let {
            GeoQuery.parseGeoQueryParameters(
                mapOf(
                    QueryParameter.GEOMETRY.key to it.geometry,
                    QueryParameter.COORDINATES.key to DataTypes.serialize(it.coordinates),
                    QueryParameter.GEOREL.key to it.georel,
                    QueryParameter.GEOPROPERTY.key to it.geoproperty
                ),
                emptyList()
            ).map { }
        } ?: Unit.right()

    companion object {

        fun deserialize(
            input: Map<String, Any>,
            contexts: List<String>
        ): Either<APIException, ServiceRegistration> =
            runCatching {
                deserializeAs<ServiceRegistration>(serializeObject(input.plus(JSONLD_CONTEXT_KW to contexts)))
                    .expand(contexts)
            }.fold(
                { it.right() },
                { it.toAPIException(serviceRegistrationFailedToParseMessage(it.message)).left() }
            )
    }
}

fun List<ServiceRegistration>.serialize(
    contexts: List<String>,
    mediaType: MediaType = JSON_LD_MEDIA_TYPE,
    includeSysAttrs: Boolean = false
): String =
    map { it.serialize(contexts, mediaType, includeSysAttrs) }.toString()
