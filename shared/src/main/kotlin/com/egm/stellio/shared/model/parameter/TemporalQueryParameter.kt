package com.egm.stellio.shared.model.parameter

enum class TemporalQueryParameter(
    override val key: String,
    override val implemented: Boolean = true,
) : QueryParameter {
    TIMEREL("timerel"),
    TIMEAT("timeAt"),
    ENDTIMEAT("endTimeAt"),
    AGGRPERIODDURATION("aggrPeriodDuration"),
    AGGRMETHODS("aggrMethods"),
    LASTN("lastN"),
    TIMEPROPERTY("timeproperty");
    companion object {
        const val WHOLE_TIME_RANGE_DURATION = "PT0S"
    }
}
