// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/functions/function_date_or_datetime_computation.h"

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionAddSeconds = FunctionDateOrDateTimeComputation<AddSecondsImpl<DataTypeDateTime>>;
using FunctionAddMinutes = FunctionDateOrDateTimeComputation<AddMinutesImpl<DataTypeDateTime>>;
using FunctionAddHours = FunctionDateOrDateTimeComputation<AddHoursImpl<DataTypeDateTime>>;
using FunctionAddDays = FunctionDateOrDateTimeComputation<AddDaysImpl<DataTypeDateTime>>;
using FunctionAddWeeks = FunctionDateOrDateTimeComputation<AddWeeksImpl<DataTypeDateTime>>;
using FunctionAddMonths = FunctionDateOrDateTimeComputation<AddMonthsImpl<DataTypeDateTime>>;
using FunctionAddQuarters = FunctionDateOrDateTimeComputation<AddQuartersImpl<DataTypeDateTime>>;
using FunctionAddYears = FunctionDateOrDateTimeComputation<AddYearsImpl<DataTypeDateTime>>;

using FunctionSubSeconds = FunctionDateOrDateTimeComputation<SubtractSecondsImpl<DataTypeDateTime>>;
using FunctionSubMinutes = FunctionDateOrDateTimeComputation<SubtractMinutesImpl<DataTypeDateTime>>;
using FunctionSubHours = FunctionDateOrDateTimeComputation<SubtractHoursImpl<DataTypeDateTime>>;
using FunctionSubDays = FunctionDateOrDateTimeComputation<SubtractDaysImpl<DataTypeDateTime>>;
using FunctionSubWeeks = FunctionDateOrDateTimeComputation<SubtractWeeksImpl<DataTypeDateTime>>;
using FunctionSubMonths = FunctionDateOrDateTimeComputation<SubtractMonthsImpl<DataTypeDateTime>>;
using FunctionSubQuarters =
        FunctionDateOrDateTimeComputation<SubtractQuartersImpl<DataTypeDateTime>>;
using FunctionSubYears = FunctionDateOrDateTimeComputation<SubtractYearsImpl<DataTypeDateTime>>;

using FunctionDateDiff =
        FunctionDateOrDateTimeComputation<DateDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionTimeDiff =
        FunctionDateOrDateTimeComputation<TimeDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionYearsDiff =
        FunctionDateOrDateTimeComputation<YearsDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionMonthsDiff =
        FunctionDateOrDateTimeComputation<MonthsDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionDaysDiff =
        FunctionDateOrDateTimeComputation<DaysDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionWeeksDiff =
        FunctionDateOrDateTimeComputation<WeeksDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionHoursDiff =
        FunctionDateOrDateTimeComputation<HoursDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionMinutesDiff =
        FunctionDateOrDateTimeComputation<MintuesDiffImpl<DataTypeDateTime, DataTypeDateTime>>;
using FunctionSecondsDiff =
        FunctionDateOrDateTimeComputation<SecondsDiffImpl<DataTypeDateTime, DataTypeDateTime>>;

using FunctionToYearWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToYearWeekTwoArgsImpl<DataTypeDateTime>>;
using FunctionToWeekTwoArgs =
        FunctionDateOrDateTimeComputation<ToWeekTwoArgsImpl<DataTypeDateTime>>;

struct NowFunctionName {
    static constexpr auto name = "now";
};

//TODO: remove the inter-layer CurrentDateTimeImpl
using FunctionNow = FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<NowFunctionName, false>>;

using FunctionNowWithPrecision =
        FunctionCurrentDateOrDateTime<CurrentDateTimeImpl<NowFunctionName, true>>;

struct CurDateFunctionName {
    static constexpr auto name = "curdate";
};

FunctionBuilderPtr createCurDateFunctionBuilderFunction() {
    return std::make_shared<CurrentDateFunctionBuilder<CurDateFunctionName>>();
}

struct CurTimeFunctionName {
    static constexpr auto name = "curtime";
};

using FunctionCurTime = FunctionCurrentDateOrDateTime<CurrentTimeImpl<CurTimeFunctionName>>;
using FunctionUtcTimeStamp = FunctionCurrentDateOrDateTime<UtcTimestampImpl>;
using FunctionTimeToSec = FunctionCurrentDateOrDateTime<TimeToSecImpl>;
using FunctionSecToTime = FunctionCurrentDateOrDateTime<SecToTimeImpl>;
using FunctionMicroSecToDateTime = TimestampToDateTime<MicroSec>;
using FunctionMilliSecToDateTime = TimestampToDateTime<MilliSec>;
using FunctionSecToDateTime = TimestampToDateTime<Sec>;

void register_function_date_time_computation(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAddSeconds>();
    factory.register_function<FunctionAddMinutes>();
    factory.register_function<FunctionAddHours>();
    factory.register_function<FunctionAddDays>();
    factory.register_function<FunctionAddWeeks>();
    factory.register_function<FunctionAddMonths>();
    factory.register_function<FunctionAddYears>();
    factory.register_function<FunctionAddQuarters>();

    factory.register_function<FunctionSubSeconds>();
    factory.register_function<FunctionSubMinutes>();
    factory.register_function<FunctionSubHours>();
    factory.register_function<FunctionSubDays>();
    factory.register_function<FunctionSubMonths>();
    factory.register_function<FunctionSubYears>();
    factory.register_function<FunctionSubQuarters>();
    factory.register_function<FunctionSubWeeks>();

    factory.register_function<FunctionDateDiff>();
    factory.register_function<FunctionTimeDiff>();
    factory.register_function<FunctionYearsDiff>();
    factory.register_function<FunctionMonthsDiff>();
    factory.register_function<FunctionWeeksDiff>();
    factory.register_function<FunctionDaysDiff>();
    factory.register_function<FunctionHoursDiff>();
    factory.register_function<FunctionMinutesDiff>();
    factory.register_function<FunctionSecondsDiff>();

    factory.register_function<FunctionNextDay>();

    factory.register_function<FunctionToYearWeekTwoArgs>();
    factory.register_function<FunctionToWeekTwoArgs>();

    factory.register_function<FunctionNow>();
    factory.register_function<FunctionNowWithPrecision>();
    factory.register_function(CurDateFunctionName::name, &createCurDateFunctionBuilderFunction);
    factory.register_function<FunctionCurTime>();
    factory.register_function<FunctionUtcTimeStamp>();
    factory.register_function<FunctionTimeToSec>();
    factory.register_function<FunctionSecToTime>();
    factory.register_function<FunctionMicroSecToDateTime>();
    factory.register_function<FunctionMilliSecToDateTime>();
    factory.register_function<FunctionSecToDateTime>();
    factory.register_function<FunctionMonthsBetween>();

    // alias
    factory.register_alias("days_add", "date_add");
    factory.register_alias("days_add", "adddate");
    factory.register_alias("months_add", "add_months");
    factory.register_alias("days_sub", "date_sub");
    factory.register_alias("days_sub", "subdate");
    factory.register_alias("now", "current_timestamp");
    factory.register_alias("now", "localtime");
    factory.register_alias("now", "localtimestamp");
    factory.register_alias("curdate", "current_date");
    factory.register_alias("curtime", "current_time");
}

} // namespace doris::vectorized
