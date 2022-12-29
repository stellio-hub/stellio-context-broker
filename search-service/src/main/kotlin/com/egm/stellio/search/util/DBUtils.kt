package com.egm.stellio.search.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.toUri
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.data.r2dbc.core.ReactiveDeleteOperation
import org.springframework.r2dbc.core.DatabaseClient
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty
import java.net.URI
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

const val DATETIME_TEMPLATE: String = "\"YYYY-MM-DD\\\"T\\\"HH24:MI:SS.US\\\"Z\\\"\""

fun DatabaseClient.GenericExecuteSpec.allToFlow(): Flow<Map<String, Any>> =
    this.fetch().all().asFlow()

suspend fun <R> DatabaseClient.GenericExecuteSpec.allToMappedList(f: (value: Map<String, Any>) -> R): List<R> =
    this.allToFlow().map { f(it) }.toList()

suspend fun <R> DatabaseClient.GenericExecuteSpec.oneToResult(
    ifEmpty: APIException = ResourceNotFoundException("Operation did not return any result"),
    f: (value: Map<String, Any>) -> R
): Either<APIException, R> =
    this.fetch()
        .one()
        .map {
            f(it).right() as Either<APIException, R>
        }
        .switchIfEmpty {
            Mono.just(ifEmpty.left())
        }
        .awaitFirst()

suspend fun DatabaseClient.GenericExecuteSpec.execute(): Either<APIException, Unit> =
    this.fetch()
        .rowsUpdated()
        .map { Unit.right() }
        .awaitFirst()

suspend fun DatabaseClient.GenericExecuteSpec.executeExpected(
    f: (value: Int) -> Either<APIException, Unit>
): Either<APIException, Unit> =
    this.fetch()
        .rowsUpdated()
        .map { f(it) }
        .awaitFirst()

suspend fun ReactiveDeleteOperation.TerminatingDelete.execute(): Either<APIException, Unit> =
    this.all()
        .map { Unit.right() }
        .awaitFirst()

fun Set<String>.toSqlArray(): String =
    "ARRAY[${this.joinToString(separator = "','", prefix = "'", postfix = "'")}]"

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
fun toJsonString(entry: Any?): String = (entry as Json).asString()
inline fun <reified T : Enum<T>> toEnum(entry: Any) = enumValueOf<T>(entry as String)
inline fun <reified T : Enum<T>> toOptionalEnum(entry: Any?) =
    (entry as? String)?.let { enumValueOf<T>(it) }
fun toInt(entry: Any?): Int = (entry as Long).toInt()
