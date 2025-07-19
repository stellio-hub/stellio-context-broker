package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.util.toUri
import io.r2dbc.postgresql.codec.Json
import java.net.URI
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime

fun toUri(entry: Any?): URI = (entry as String).toUri()
fun toNullableUri(entry: Any?): URI? = (entry as? String)?.toUri()
fun toBoolean(entry: Any?): Boolean = entry as Boolean
fun toZonedDateTime(entry: Any?): ZonedDateTime =
    (entry as OffsetDateTime).atZoneSameInstant(ZoneOffset.UTC)
fun toNullableZonedDateTime(entry: Any?): ZonedDateTime? =
    (entry as? OffsetDateTime)?.atZoneSameInstant(ZoneOffset.UTC)
fun <T> toList(entry: Any): List<T> = (entry as Array<T>).toList()
fun <T> toNullableList(entry: Any?): List<T>? = (entry as? Array<T>)?.toList()
fun toJsonString(entry: Any?): String? = (entry as? Json)?.asString()
inline fun <reified T : Enum<T>> toEnum(entry: Any) = enumValueOf<T>(entry as String)
inline fun <reified T : Enum<T>> toOptionalEnum(entry: Any?) =
    (entry as? String)?.let { enumValueOf<T>(it) }
fun toInt(entry: Any?): Int = (entry as Long).toInt()
fun toNullableInt(entry: Any?): Int? = entry as? Int

fun String.toSqlColumnName(): String =
    this.map {
        if (it.isUpperCase()) "_${it.lowercase()}"
        else it
    }.joinToString("")

fun Any.toSqlValue(columnName: String): Any? =
    when (columnName) {
        "watchedAttributes", "contexts" -> {
            val valueAsArrayList = this as ArrayList<String>
            if (valueAsArrayList.isEmpty())
                null
            else
                valueAsArrayList.joinToString(separator = ",")
        }
        "notificationTrigger" -> (this as ArrayList<String>).toTypedArray()
        else -> this
    }
