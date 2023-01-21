package com.egm.stellio.subscription.utils

import arrow.core.Either
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.r2dbc.core.DatabaseClient
import java.net.URI

suspend fun DatabaseClient.GenericExecuteSpec.execute(): Either<APIException, Unit> =
    this.fetch()
        .rowsUpdated()
        .map { Unit.right() }
        .awaitFirst()

fun toUri(entry: Any?): URI = (entry as String).toUri()
fun <T> toList(entry: Any?): List<T> = (entry as Array<T>).toList()
