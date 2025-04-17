package com.egm.stellio.search.authorization.permission.model

enum class Action(val value: String) {
    READ("read"),
    WRITE("write"),
    ADMIN("admin")
}
