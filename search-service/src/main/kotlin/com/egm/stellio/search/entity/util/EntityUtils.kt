package com.egm.stellio.search.entity.util

import com.egm.stellio.search.common.util.*
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy

fun Map<String, Any>.rowToEntity(): Entity =
    Entity(
        entityId = toUri(this["entity_id"]),
        types = toList(this["types"]),
        scopes = toOptionalList(this["scopes"]),
        createdAt = toZonedDateTime(this["created_at"]),
        modifiedAt = toOptionalZonedDateTime(this["modified_at"]),
        payload = toJson(this["payload"]),
        specificAccessPolicy = toOptionalEnum<SpecificAccessPolicy>(this["specific_access_policy"])
    )
