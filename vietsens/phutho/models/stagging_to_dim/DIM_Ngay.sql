
{{ config(order_by='MaNgay', engine='Join(ANY, LEFT, MaNgay)', materialized='table') }}
with aaaa as(
select  toInt64(SUBSTRING(replaceAll(toString(Ngay), '-',''),1,8)) as MaNgay,arrayJoin(arrayMap(x ->toDate( parseDateTimeBestEffort('2009-01-01'))+ interval x day, range(0, 30000, 1))) as Ngay,
toDayOfWeek(Ngay) as DayOfWeek, DAYOFMONTH(Ngay) as DayOfMonth, DAYOFYEAR(Ngay) as DayOfYear, 
toWeek(Ngay,9) as WeekOfYear,toMonth(Ngay) as MonthOfYear, toQuarter(Ngay) as QuarterofYear
)
select * from aaaa

