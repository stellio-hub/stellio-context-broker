package com.egm.stellio.search.util

import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.deserializeExpandedPayload
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toUri
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

fun toUri(entry: Any?): URI = (entry as String).toUri()
fun toOptionalUri(entry: Any?): URI? = (entry as? String)?.toUri()
fun toUuid(entry: Any?): UUID = entry as UUID
fun toBoolean(entry: Any?): Boolean = entry as Boolean
fun toZonedDateTime(entry: Any?): ZonedDateTime =
    (entry as OffsetDateTime).atZoneSameInstant(ZoneOffset.UTC)
fun toOptionalZonedDateTime(entry: Any?): ZonedDateTime? =
    (entry as? OffsetDateTime)?.atZoneSameInstant(ZoneOffset.UTC)
fun <T> toList(entry: Any?): List<T> = (entry as Array<T>).toList()
fun <T> toOptionalList(entry: Any?): List<T>? = (entry as? Array<T>)?.toList()
fun toJson(entry: Any?): Json = entry as Json
fun toJsonString(entry: Any?): String = (entry as Json).asString()
inline fun <reified T : Enum<T>> toEnum(entry: Any) = enumValueOf<T>(entry as String)
inline fun <reified T : Enum<T>> toOptionalEnum(entry: Any?) =
    (entry as? String)?.let { enumValueOf<T>(it) }
fun toInt(entry: Any?): Int = (entry as Long).toInt()

fun Json.deserializeExpandedPayload(): Map<String, List<Any>> = this.asString().deserializeExpandedPayload()
fun Json.deserializeAsMap(): Map<String, Any> = this.asString().deserializeAsMap()

fun ExpandedAttributeInstance.toJson(): Json = Json.of(serializeObject(this))
