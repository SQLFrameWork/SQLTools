DECLARE @JobName nvarchar(1024) = 'syspolicy_purge_history'

DROP TABLE IF EXISTS #Table1
DROP TABLE IF EXISTS #Table2
DROP TABLE IF EXISTS #Table3

SET DATEFIRST 1; -- Monday = 1

;WITH Anchor AS
(
    SELECT 
        Today   = CAST(GETDATE() AS date),
        Base140 = DATEADD(day, -140, CAST(GETDATE() AS date))
),
Bounds AS
(
    SELECT
        WeekStart = DATEADD(day, -(DATEPART(weekday, Today) - 1), Today),      -- Monday of current week
        StartDate = DATEADD(day, -(DATEPART(weekday, Base140) - 1), Base140),  -- Monday before Today-140
        EndDate   = DATEADD(day, 7 - DATEPART(weekday, Today), Today)          -- Next Sunday after Today
    FROM Anchor
),
Dates AS
(
    SELECT StartDate AS DateValue
    FROM Bounds
    UNION ALL
    SELECT DATEADD(day, 1, d.DateValue)
    FROM Dates d
    CROSS JOIN Bounds b
    WHERE d.DateValue < b.EndDate
)
SELECT
    WeekNoRel = DATEDIFF(week, DateValue, (SELECT WeekStart FROM Bounds)) + 1, -- 1=current week, 2=last, …
    WeekDay   = DATEPART(weekday, DateValue),                                  -- 1=Mon … 7=Sun
    [Date]    = DateValue
INTO #Table1
FROM Dates
OPTION (MAXRECURSION 2000);

;WITH JobRuns AS
(
    SELECT 
        run_date = CONVERT(date, CONVERT(char(8), h.run_date)), -- yyyymmdd → date
        run_duration_seconds =
              ((h.run_duration / 10000) * 3600)          -- hours
            + (((h.run_duration % 10000) / 100) * 60)     -- minutes
            +  (h.run_duration % 100)                     -- seconds
    FROM msdb.dbo.sysjobhistory h
    INNER JOIN msdb.dbo.sysjobs j
        ON h.job_id = j.job_id
    WHERE j.name = @JobName 
      AND h.step_id = 0 -- only overall job outcome
)
SELECT
    [Date]      = run_date,
    MaxDuration = MAX(run_duration_seconds)
INTO #Table2
FROM JobRuns
GROUP BY run_date;

;WITH Joined AS
(
    SELECT 
        t1.WeekNoRel,
        t1.WeekDay,
        ISNULL(t2.MaxDuration, 0) AS Duration
    FROM #Table1 t1
    LEFT JOIN #Table2 t2
        ON t1.[Date] = t2.[Date]
),
Aggregated AS
(
    SELECT WeekDay, WeekNoRel, MAX(Duration) AS Duration
    FROM Joined
    GROUP BY WeekDay, WeekNoRel
)
SELECT WeekDay,
       ISNULL([1],0)  AS Week1,
       ISNULL([2],0)  AS Week2,
       ISNULL([3],0)  AS Week3,
       ISNULL([4],0)  AS Week4,
       ISNULL([5],0)  AS Week5,
       ISNULL([6],0)  AS Week6,
       ISNULL([7],0)  AS Week7,
       ISNULL([8],0)  AS Week8,
       ISNULL([9],0)  AS Week9,
       ISNULL([10],0) AS Week10,
       ISNULL([11],0) AS Week11,
       ISNULL([12],0) AS Week12,
       ISNULL([13],0) AS Week13,
       ISNULL([14],0) AS Week14,
       ISNULL([15],0) AS Week15,
       ISNULL([16],0) AS Week16,
       ISNULL([17],0) AS Week17,
       ISNULL([18],0) AS Week18,
       ISNULL([19],0) AS Week19,
       ISNULL([20],0) AS Week20
       INTO #Table3
FROM Aggregated
PIVOT
(
    MAX(Duration) FOR WeekNoRel IN (
        [1],[2],[3],[4],[5],[6],[7],[8],[9],[10],
        [11],[12],[13],[14],[15],[16],[17],[18],[19],[20]
    )
) AS pvt
ORDER BY WeekDay;

select @JobName JobName
,CASE WHEN [weekday]= 1 THEN 'Monday'
WHEN [weekday]= 2 THEN 'Tuesday'
WHEN [weekday]= 3 THEN 'Wednesday'
WHEN [weekday]= 4 THEN 'Thursday'
WHEN [weekday]= 5 THEN 'Friday'
WHEN [weekday]= 6 THEN 'Saturday'
WHEN [weekday]= 7 THEN 'Sunday'
ELSE '' END 'DayOfWeek'
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week1, 0), 108)  [CurrentWeek (1) ]
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week2, 0), 108)  Week2
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week3, 0), 108) Week3
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week4, 0), 108) Week4
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week5, 0), 108)Week5
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week6, 0), 108)Week6
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week7, 0), 108)Week7
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week8, 0), 108)Week8
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week9, 0), 108)Week9
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week10, 0), 108)Week10
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week11, 0), 108)Week11
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week12, 0), 108)Week12
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week13, 0), 108)Week13
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week14, 0), 108)Week14
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week15, 0), 108)Week15
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week16, 0), 108)Week16
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week16, 0), 108)Week17
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week18, 0), 108)Week18
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week19, 0), 108)Week19
,CONVERT(VARCHAR(8), DATEADD(SECOND, Week20, 0), 108)Week20
from #Table3

