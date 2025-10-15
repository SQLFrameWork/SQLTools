
/****** Object:  StoredProcedure [dbo].[Get_AuditHistory]    Script Date: 15/10/2025 12:01:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[Get_SysJobs]
AS
BEGIN

/*

### Procedure: Get_SysJobs
**Author:**       Mark.Rollinson@SQLFramework.com  
**Create date:**  13/08/2024  
**Notes:**  
- Get list of current msdb..SysJobs 
- INSERT / UPDATE / DELETE into local SysJobs table 
- Hashes created to capture command and shedule changes to simply comparisons 

*/


SELECT 
    @@Servername SQLInstance 
    ,j.name JobName
    ,js.step_id
    ,j.enabled JobEnabled
    ,HASHBYTES('SHA2_256',js.command) CommandHash
    ,ISNULL(s.enabled ,0)  SchedEnabled
    ,HASHBYTES('SHA2_256',CONCAT (s.freq_type,s.freq_interval,s.freq_subday_type,s.freq_subday_interval,s.freq_relative_interval,s.freq_recurrence_factor,0) ) ScheduleHash
    INTO #CurrentJobs
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.sysjobschedules jsch   ON j.job_id = jsch.job_id
LEFT JOIN msdb.dbo.sysschedules s           ON jsch.schedule_id = s.schedule_id
LEFT JOIN msdb.dbo.sysjobsteps js           ON j.job_id = js.job_id


-- UPDATE Changes to existing Job Steps 

UPDATE t
SET 
    t.JobEnabled= s.JobEnabled
    ,t.CommandHash= s.CommandHash
    ,t.SchedEnabled= s.SchedEnabled
    ,t.ScheduleHash= s.ScheduleHash
    ,t.LastAction = 'UPDATE'
    ,t.LastChangeDate = getdate()
FROM dbo.SysJobs t
JOIN #CurrentJobs s ON t.SQLInstance  = s.SQLInstance 
                    AND t.JobName = s.JobName
                    AND t.Step_ID= s.Step_ID
WHERE   t.JobEnabled <> s.JobEnabled
    OR  ISNULL(t.CommandHash, 0x00) <> ISNULL(s.CommandHash, 0x00)
    OR  t.SchedEnabled <> s.SchedEnabled
    OR ISNULL(t.ScheduleHash, 0x00) <> ISNULL(s.ScheduleHash, 0x00)


-- INSERT New Job steps 

INSERT INTO dbo.SysJobs 
SELECT * 
    ,'INSERT' 
    ,getdate() 
FROM #CurrentJobs  s
WHERE NOT EXISTS (
    SELECT 1 
    FROM dbo.SysJobs t
    WHERE t.SQLInstance = s.SQLInstance
      AND t.JobName = s.JobName
      AND t.step_id= s.step_id
);

-- Flag DELETED jobsteps 

UPDATE t
SET 
    t.LastAction = 'DELETED'
    ,t.LastChangeDate = getdate()
FROM dbo.SysJobs t
WHERE NOT EXISTS (
    SELECT 1
    FROM #CurrentJobs s
    WHERE t.SQLInstance = s.SQLInstance
      AND t.JobName = s.JobName
      AND t.step_id= s.step_id
);


END 

GO 

--SELECT SQLInstance      ,JobName      ,step_id       ,JobEnabled      ,CommandHash      ,SchedEnabled      ,ScheduleHash       ,LastAction      ,LastChangeDate  FROM DBA_Audit..SysJobs



------


/****** Object:  StoredProcedure [dbo].[Get_AuditHistory]    Script Date: 15/10/2025 12:01:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   OR ALTER PROCEDURE [dbo].[Rpt_SysJobs]
AS
BEGIN

/*

### Procedure: Rpt_SysJobs
**Author:**       Mark.Rollinson@SQLFramework.com  
**Create date:**  13/08/2024  
**Notes:**  
- Report any out of sync job steps by comparing msdb..SysJobs  to local SysJobs table 
*/



DROP TABLE IF EXISTS #CurrentJobs

SELECT 
    @@Servername SQLInstance 
    ,j.name JobName
    ,js.step_id
    ,j.enabled JobEnabled
    ,HASHBYTES('SHA2_256',js.command) CommandHash
    ,ISNULL(s.enabled ,0)  SchedEnabled
    ,HASHBYTES('SHA2_256',CONCAT (s.freq_type,s.freq_interval,s.freq_subday_type,s.freq_subday_interval,s.freq_relative_interval,s.freq_recurrence_factor,0) ) ScheduleHash
    INTO #CurrentJobs
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.sysjobschedules jsch   ON j.job_id = jsch.job_id
LEFT JOIN msdb.dbo.sysschedules s           ON jsch.schedule_id = s.schedule_id
LEFT JOIN msdb.dbo.sysjobsteps js           ON j.job_id = js.job_id

-- SCHEDULE CHANGES 

;WITH CTE AS 
(
SELECT 
    ISNULL(l.SQLInstance, t.SQLInstance) SQLInstance
    ,ISNULL(l.JobName, t.JobName)       JobName
    ,ISNULL(l.step_id, t.step_id)       Step_id
    ,CASE WHEN l.JobEnabled<>t.JobEnabled THEN 'OutOfSync' Else 'ok' END JobEnabledStatus
    ,CASE WHEN l.SchedEnabled<>t.SchedEnabled THEN 'OutOfSync' Else 'ok' END ScheduleEnabled
    ,CASE WHEN l.CommandHash<>t.CommandHash THEN 'OutOfSync' Else 'ok' END CommandDetail
    ,CASE WHEN l.ScheduleHash<>t.ScheduleHash THEN 'OutOfSync' Else 'ok' END ScheduleDetail
FROM #CurrentJobs l
FULL OUTER JOIN dbo.SysJobs t
    ON l.SQLInstance = t.SQLInstance
    AND l.JobName = t.JobName
    AND ISNULL(l.step_id, -1) = ISNULL(t.step_id, -1)
WHERE
    (   l.JobName IS NULL        -- exists in table but missing in msdb
        OR t.JobName IS NULL        -- exists in msdb but missing in table
        OR l.CommandHash<> t.CommandHash
    )
    AND ISNULL(LastAction,'')      !='DELETED'

)

SELECT * FROM CTE 
UNION 
SELECT 
    ISNULL(l.SQLInstance, t.SQLInstance) SQLInstance
    ,ISNULL(l.JobName, t.JobName)       JobName
    ,''                                 Step_id
    ,CASE WHEN l.JobEnabled<>t.JobEnabled THEN 'OutOfSync' Else 'ok' END JobEnabledStatus
    ,CASE WHEN l.SchedEnabled<>t.SchedEnabled THEN 'OutOfSync' Else 'ok' END ScheduleEnabled
    ,'ok'                                 CommandDetail
    ,CASE WHEN l.ScheduleHash<>t.ScheduleHash THEN 'OutOfSync' Else 'ok' END ScheduleDetail
FROM #CurrentJobs l
FULL OUTER JOIN dbo.SysJobs t
    ON l.SQLInstance = t.SQLInstance
    AND l.JobName = t.JobName
    AND ISNULL(l.step_id, -1) = ISNULL(t.step_id, -1)
WHERE
    (   l.JobName IS NULL        -- exists in table but missing in msdb
        OR t.JobName IS NULL        -- exists in msdb but missing in table
        OR l.JobEnabled <> t.JobEnabled
        OR l.SchedEnabled <> t.SchedEnabled
        --OR l.CommandHash<> t.CommandHash
        OR l.SchedEnabled <> t.SchedEnabled
        OR l.ScheduleHash<> t.ScheduleHash
    )
    AND ISNULL(LastAction,'')      !='DELETED'
    AND ISNULL(l.JobName, t.JobName)        NOT IN (SELECT JobName FROM CTE) 





END 

-- IF PRIMARY 
IF EXISTS ( SELECT sys.fn_hadr_is_primary_replica(name) FROM sys.databases 
            WHERE replica_id IS NOT Null and sys.fn_hadr_is_primary_replica(name) =1 )
EXEC Get_SysJobs

ELSE IF EXISTS ( SELECT sys.fn_hadr_is_primary_replica(name) FROM sys.databases 
            WHERE replica_id IS NOT Null and sys.fn_hadr_is_primary_replica(name) =0 )
EXEC Rpt_SysJobs

ELSE IF NOT EXISTS (SELECT sys.fn_hadr_is_primary_replica(name)    FROM sys.databases WHERE replica_id IS NOT Null)
PRINT 'Do Nothing' 
