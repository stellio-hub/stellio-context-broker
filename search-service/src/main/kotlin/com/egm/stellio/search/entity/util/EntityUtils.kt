package com.egm.stellio.search.entity.util

import com.egm.stellio.search.common.util.toJson
import com.egm.stellio.search.common.util.toList
import com.egm.stellio.search.common.util.toOptionalList
import com.egm.stellio.search.common.util.toOptionalZonedDateTime
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.common.util.toZonedDateTime
import com.egm.stellio.search.entity.model.Entity

fun Map<String, Any>.rowToEntity(): Entity =
    Entity(
        entityId = toUri(this["entity_id"]),
        types = toList(this["types"]),
        scopes = toOptionalList(this["scopes"]),
        createdAt = toZonedDateTime(this["created_at"]),
        modifiedAt = toZonedDateTime(this["modified_at"]),
        deletedAt = toOptionalZonedDateTime(this["deleted_at"]),
        payload = toJson(this["payload"]),
    )
