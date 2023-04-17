package com.egm.stellio.search.util

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.model.TemporalQuery

fun aggrMethodToSqlAggregate(
    aggregate: TemporalQuery.Aggregate,
    attributeValueType: TemporalEntityAttribute.AttributeValueType
): String = when (attributeValueType) {
    TemporalEntityAttribute.AttributeValueType.STRING -> sqlAggregationForJsonString(aggregate)
    TemporalEntityAttribute.AttributeValueType.NUMBER -> sqlAggregateForJsonNumber(aggregate)
    TemporalEntityAttribute.AttributeValueType.OBJECT -> sqlAggregateForJsonObject(aggregate)
    TemporalEntityAttribute.AttributeValueType.ARRAY -> sqlAggregateForJsonArray(aggregate)
    TemporalEntityAttribute.AttributeValueType.BOOLEAN -> sqlAggregateForJsonBoolean(aggregate)
    TemporalEntityAttribute.AttributeValueType.DATETIME -> sqlAggregateForDateTime(aggregate)
    TemporalEntityAttribute.AttributeValueType.DATE -> sqlAggregateForDate(aggregate)
    TemporalEntityAttribute.AttributeValueType.TIME -> sqlAggregateForTime(aggregate)
    TemporalEntityAttribute.AttributeValueType.URI -> sqlAggregateForURI(aggregate)
    TemporalEntityAttribute.AttributeValueType.GEOMETRY -> "null"
}

fun sqlAggregationForJsonString(aggregate: TemporalQuery.Aggregate): String = when (aggregate) {
    TemporalQuery.Aggregate.TOTAL_COUNT -> "count(value)"
    TemporalQuery.Aggregate.DISTINCT_COUNT -> "count(distinct(value))"
    TemporalQuery.Aggregate.MIN -> "min(value)"
    TemporalQuery.Aggregate.MAX -> "max(value)"
    else -> "null"
}

private fun sqlAggregateForJsonNumber(aggregate: TemporalQuery.Aggregate): String = when (aggregate) {
    TemporalQuery.Aggregate.TOTAL_COUNT -> "count(measured_value)"
    TemporalQuery.Aggregate.DISTINCT_COUNT -> "count(distinct(measured_value))"
    TemporalQuery.Aggregate.SUM -> "sum(measured_value)"
    TemporalQuery.Aggregate.AVG -> "avg(measured_value)"
    TemporalQuery.Aggregate.MIN -> "min(measured_value)"
    TemporalQuery.Aggregate.MAX -> "max(measured_value)"
    TemporalQuery.Aggregate.STDDEV -> "stddev_samp(measured_value)"
    TemporalQuery.Aggregate.SUMSQ -> "sum(power(measured_value,2))"
}

fun sqlAggregateForJsonObject(aggregate: TemporalQuery.Aggregate): String = when (aggregate) {
    TemporalQuery.Aggregate.TOTAL_COUNT -> "count(value)"
    TemporalQuery.Aggregate.DISTINCT_COUNT -> "count(distinct(value))"
    else -> "null"
}

fun sqlAggregateForJsonArray(aggregate: TemporalQuery.Aggregate): String = when (aggregate) {
    TemporalQuery.Aggregate.TOTAL_COUNT -> "count(value)"
    TemporalQuery.Aggregate.DISTINCT_COUNT -> "count(distinct(value))"
    TemporalQuery.Aggregate.SUM -> "sum(jsonb_array_length(value::jsonb))"
    TemporalQuery.Aggregate.AVG -> "round(avg(jsonb_array_length(value::jsonb)), 5)"
    TemporalQuery.Aggregate.MIN -> "min(jsonb_array_length(value::jsonb))"
    TemporalQuery.Aggregate.MAX -> "max(jsonb_array_length(value::jsonb))"
    else -> "null"
}

fun sqlAggregateForJsonBoolean(aggregate: TemporalQuery.Aggregate): String = when (aggregate) {
    TemporalQuery.Aggregate.TOTAL_COUNT -> "count(value)"
    TemporalQuery.Aggregate.DISTINCT_COUNT -> "count(distinct(value))"
    TemporalQuery.Aggregate.SUM -> "sum(case when value::boolean then 1 else 0 end)"
    TemporalQuery.Aggregate.AVG -> "round(avg(case when value::boolean then 1 else 0 end), 5)"
    TemporalQuery.Aggregate.MIN -> "min(case when value::boolean then 1 else 0 end)"
    TemporalQuery.Aggregate.MAX -> "max(case when value::boolean then 1 else 0 end)"
    TemporalQuery.Aggregate.STDDEV -> "stddev_samp(case when value::boolean then 1 else 0 end)"
    TemporalQuery.Aggregate.SUMSQ -> "sum(power(case when value::boolean then 1 else 0 end,2))"
}

fun sqlAggregateForDateTime(aggregate: TemporalQuery.Aggregate): String = when (aggregate) {
    TemporalQuery.Aggregate.TOTAL_COUNT -> "count(value)"
    TemporalQuery.Aggregate.DISTINCT_COUNT -> "count(distinct(value))"
    TemporalQuery.Aggregate.MIN -> "min(value)"
    TemporalQuery.Aggregate.MAX -> "max(value)"
    else -> "null"
}

fun sqlAggregateForDate(aggregate: TemporalQuery.Aggregate): String = when (aggregate) {
    TemporalQuery.Aggregate.TOTAL_COUNT -> "count(value)"
    TemporalQuery.Aggregate.DISTINCT_COUNT -> "count(distinct(value))"
    TemporalQuery.Aggregate.MIN -> "min(value)"
    TemporalQuery.Aggregate.MAX -> "max(value)"
    else -> "null"
}

fun sqlAggregateForTime(aggregate: TemporalQuery.Aggregate): String = when (aggregate) {
    TemporalQuery.Aggregate.TOTAL_COUNT -> "count(value)"
    TemporalQuery.Aggregate.DISTINCT_COUNT -> "count(distinct(value))"
    TemporalQuery.Aggregate.AVG -> "time with time zone '00:00:00Z' + avg(value::time)"
    TemporalQuery.Aggregate.MIN -> "min(value)"
    TemporalQuery.Aggregate.MAX -> "max(value)"
    else -> "null"
}

fun sqlAggregateForURI(aggregate: TemporalQuery.Aggregate): String = when (aggregate) {
    TemporalQuery.Aggregate.TOTAL_COUNT -> "count(value)"
    TemporalQuery.Aggregate.DISTINCT_COUNT -> "count(distinct(value))"
    else -> "null"
}
