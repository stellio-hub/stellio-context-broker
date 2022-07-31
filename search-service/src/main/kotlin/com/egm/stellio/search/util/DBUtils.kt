package com.egm.stellio.search.util

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.JsonLdUtils.logger
import com.egm.stellio.shared.util.entityNotFoundMessage
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.data.r2dbc.core.ReactiveDeleteOperation
import org.springframework.r2dbc.core.DatabaseClient
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

fun dbOperationErrorMessage(throwable: Throwable) = "Error while executing DB operation: $throwable"

fun dbOperationError(throwable: Throwable): APIException =
    InternalErrorException(dbOperationErrorMessage(throwable))

fun DatabaseClient.GenericExecuteSpec.allToFlow(): Flow<Map<String, Any>> =
    this.fetch().all().asFlow()

suspend fun <R> DatabaseClient.GenericExecuteSpec.allToMappedList(f: (value: Map<String, Any>) -> R): List<R> =
    this.allToFlow().map { f(it) }.toList()

suspend fun <R> DatabaseClient.GenericExecuteSpec.oneToResult(
    f: (value: Map<String, Any>) -> R
): Either<APIException, R> =
    this.fetch()
        .one()
        .map {
            f(it).right() as Either<APIException, R>
        }
        .switchIfEmpty {
            Mono.just(ResourceNotFoundException(entityNotFoundMessage("")).left())
        }
        .onErrorResume {
            logger.error(dbOperationErrorMessage(it))
            Mono.just(dbOperationError(it).left())
        }
        .awaitFirst()

suspend fun DatabaseClient.GenericExecuteSpec.execute(): Either<APIException, Unit> =
    this.fetch()
        .rowsUpdated()
        .map { Unit.right() as Either<APIException, Unit> }
        .onErrorResume {
            logger.error(dbOperationErrorMessage(it))
            Mono.just(dbOperationError(it).left())
        }
        .awaitFirst()

suspend fun DatabaseClient.GenericExecuteSpec.executeExpected(
    f: (value: Int) -> Either<APIException, Unit>
): Either<APIException, Unit> =
    this.fetch()
        .rowsUpdated()
        .map { f(it) }
        .onErrorResume {
            logger.error(dbOperationErrorMessage(it))
            Mono.just(dbOperationError(it).left())
        }
        .awaitFirst()

suspend fun ReactiveDeleteOperation.TerminatingDelete.execute(): Either<APIException, Unit> =
    this.all()
        .map { Unit.right() as Either<APIException, Unit> }
        .onErrorResume {
            logger.error(dbOperationErrorMessage(it))
            Mono.just(dbOperationError(it).left())
        }
        .awaitFirst()
