package com.egm.stellio.search.authorization

import io.r2dbc.postgresql.codec.Json

internal fun getSubjectInfoForUser(username: String): Json =
    Json.of(
        """
        { "type": "Property", "value": { "username": "$username" } }
        """.trimIndent()
    )

internal fun getSubjectInfoForGroup(name: String): Json =
    Json.of(
        """
        { "type": "Property", "value": { "name": "$name" } }
        """.trimIndent()
    )

internal fun getSubjectInfoForClient(clientId: String): Json =
    Json.of(
        """
        { "type": "Property", "value": { "clientId": "$clientId" } }
        """.trimIndent()
    )
