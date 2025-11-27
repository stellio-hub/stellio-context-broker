package com.egm.stellio.search.authorization.permission.support

import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.UriUtils.toUri
import java.net.URI
import java.util.UUID

object PermissionUtils {
    fun gimmeRawPermission(
        id: URI = "urn:ngsi-ld:Permission:${UUID.randomUUID()}".toUri(),
        target: TargetAsset = TargetAsset(id = "my:id".toUri()),
        assignee: Sub = MOCK_USER_SUB,
        action: Action = Action.READ,
        assigner: Sub = MOCK_USER_SUB,
    ) = Permission(id, target = target, assignee = assignee, action = action, assigner = assigner)
}
