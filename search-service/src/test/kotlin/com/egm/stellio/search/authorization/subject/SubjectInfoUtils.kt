package com.egm.stellio.search.authorization.subject

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

internal fun getSubjectInfoForClient(clientId: String, kcId: String): Json =
    Json.of(
        """
        { "type": "Property", "value": { "clientId": "$clientId", "internalClientId": "$kcId" } }
        """.trimIndent()
    )

const val USER_UUID = "0768A6D5-D87B-4209-9A22-8C40A8961A79"
const val SERVICE_ACCOUNT_UUID = "3CB80121-B2D6-4F76-BE97-B459CCC3AF72"
const val GROUP_UUID = "52A916AB-19E6-4D3B-B629-936BC8E5B640"
