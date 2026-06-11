package com.egm.stellio.shared.queryparameter

sealed class QNode

data class AndNode(val left: QNode, val right: QNode) : QNode()
data class OrNode(val left: QNode, val right: QNode) : QNode()
data class NotExistsNode(val rawPath: String) : QNode()
data class ExistsNode(val rawPath: String) : QNode()
data class ComparisonNode(
    val rawPath: String,
    val operator: ComparisonOperator,
    val value: QValue
) : QNode()

enum class ComparisonOperator(val ngsildOp: String, val sqlOp: String) {
    EQ("==", "=="),
    NEQ("!=", "<>"),
    GTE(">=", ">="),
    GT(">", ">"),
    LTE("<=", "<="),
    LT("<", "<"),
    LIKE_REGEX("~=", "like_regex"),
    NOT_LIKE_REGEX("!~=", "not_like_regex")
}
