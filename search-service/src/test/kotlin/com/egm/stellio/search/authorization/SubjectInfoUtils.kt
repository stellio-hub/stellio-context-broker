package com.egm.stellio.search.authorization

internal fun getSubjectInfoForUser(username: String): String =
    """
    { "type": "Property", "value": { "username": "$username" } }
    """.trimIndent()

internal fun getSubjectInfoForGroup(name: String): String =
    """
    { "type": "Property", "value": { "name": "$name" } }
    """.trimIndent()

internal fun getSubjectInfoForClient(clientId: String): String =
    """
    { "type": "Property", "value": { "clientId": "$clientId" } }
    """.trimIndent()
