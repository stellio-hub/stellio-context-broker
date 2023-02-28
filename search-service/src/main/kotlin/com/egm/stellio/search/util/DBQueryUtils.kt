package com.egm.stellio.search.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ResourceNotFoundException
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
    f: (value: Long) -> Either<APIException, Unit>
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

fun URI?.toDatasetIdFilter(): String =
    if (this != null) "AND dataset_id = :dataset_id"
    else "AND dataset_id IS NULL"

fun String.wrapToAndClause(clause: String): String =
    this.plus(" AND (").plus(clause).plus(")")
