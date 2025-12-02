SET NOCOUNT ON 

DROP table if exists #JobFailures
DROP table if exists #LogBloat
DROP table if exists #StorageStatus
DROP table if exists #AGHealth
DROP table if exists #AGJObSync
DROP table if exists #AGLoginSync
DROP table if exists #AGFailover
DROP table if exists #SQLrestart
DROP table if exists #q
DROP table if exists #Backups 
DROP TABLE IF EXISTS #Backups1

CREATE TABLE #Backups (	DBname varchar(128), BackupDevice varchar(1024), SizeDB int, SecDuration bigint 
						,RecoveryModel varchar(128), BackupType varchar(128), Completed datetime , DaysOld int )

INSERT #Backups 
EXEC Rpt_LastBackup ''

DELETE #Backups  WHERE DaysOld <=1

select top 3 
    @@SERVERNAME    ServerName
    ,DBname	, BackupDevice	, Completed , DaysOld
INTO #Backups1
FROM #Backups 

-- SQL Agent Failed Jobs 
SELECT top 2
    @@SERVERNAME    ServerName,
    j.name          Job,
    js.step_id      StepID,
    js.step_name    StepName,
    jh.sql_severity Severity,
    LEFT(jh.message,50)+'....' ErrorMessage,
    msdb.dbo.agent_datetime(jh.run_date, jh.run_time) RunTime
    INTO #JobFailures
FROM msdb.dbo.sysjobs j
JOIN msdb.dbo.sysjobsteps js ON js.job_id = j.job_id
JOIN msdb.dbo.sysjobhistory jh ON jh.job_id = j.job_id
WHERE jh.run_status = 0
and jh.step_id>0
AND msdb.dbo.agent_datetime(jh.run_date, jh.run_time) > GETDATE() - 1
ORDER BY j.name          , msdb.dbo.agent_datetime(jh.run_date, jh.run_time) 


-- Trans Log Bloat
SELECT     @@SERVERNAME    ServerName,
    d.name AS DatabaseName,
    SUM(CASE WHEN mf.type_desc = 'ROWS' THEN cast(mf.size * 8.0 / 1024 as int) ELSE 0 END) AS DataFileSizeMB,
    SUM(CASE WHEN mf.type_desc = 'LOG' THEN cast(mf.size * 8.0 / 1024 as int) ELSE 0 END) AS LogFileSizeMB
    INTO #LogBloat
FROM 
    sys.master_files mf
JOIN 
    sys.databases d ON mf.database_id = d.database_id
WHERE 
    d.state_desc = 'ONLINE'  -- Optional: only online databases
GROUP BY 
    d.name
HAVING 
    SUM(CASE WHEN mf.type_desc = 'LOG' THEN mf.size * 8.0 / 1024 ELSE 0 END) >
    SUM(CASE WHEN mf.type_desc = 'ROWS' THEN mf.size * 8.0 / 1024 ELSE 0 END)
ORDER BY 
    LogFileSizeMB DESC;

-- Storage  Status
SELECT     @@SERVERNAME    ServerName,
    vs.volume_mount_point AS Drive,
    MAX(vs.total_bytes) / 1024 / 1024 / 1024 AS TotalSpaceGB,
    MAX(vs.available_bytes) / 1024 / 1024 / 1024 AS FreeSpaceGB,
    (MAX(vs.total_bytes) - MAX(vs.available_bytes)) / 1024 / 1024 / 1024 AS UsedSpaceGB
    ,cast((cast(MAX(vs.available_bytes) as float ) /MAX(vs.total_bytes))*100 as int)  PercentageFree
    INTO #StorageStatus
FROM sys.master_files AS f
CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS vs
GROUP BY vs.volume_mount_point
HAVING cast((cast(MAX(vs.available_bytes) as float ) /MAX(vs.total_bytes))*100 as int)  <15
ORDER BY vs.volume_mount_point;


-- Availability Group Health
SELECT     @@SERVERNAME    ServerName,
    ag.name AS [AG Name],
    ar.replica_server_name AS [Replica],
    ars.role_desc AS [Role],
    ars.connected_state_desc AS [Connection State],
    drs.synchronization_state_desc AS [Sync State],
    drs.synchronization_health_desc AS [Health]
    INTO #AGHealth
FROM 
    sys.availability_groups ag
JOIN 
    sys.availability_replicas ar ON ag.group_id = ar.group_id
JOIN 
    sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id
JOIN 
    sys.dm_hadr_database_replica_states drs ON ar.replica_id = drs.replica_id
GROUP BY 
    ag.name, ar.replica_server_name, ars.role_desc, ars.connected_state_desc, 
    drs.synchronization_state_desc, drs.synchronization_health_desc;



-- Availability Group Job Sync
select     @@SERVERNAME    ServerName,'Backups ' JobName , 'Out of Sync' Status 
INTO #AGJObSync


-- Availability Group Login Sync
select     @@SERVERNAME    ServerName,'Backups ' LoginName, 'Out of Sync' Status 
INTO #AGLoginSync

-- Availability Failover
select     @@SERVERNAME    ServerName,sqlserver_start_time 
INTO #AGFailover
 from sys.dm_os_sys_info

-- Last SQL Restart 
select     @@SERVERNAME    ServerName,sqlserver_start_time 
INTO #SQLrestart
 from sys.dm_os_sys_info




    DECLARE @StatusFlag varchar(8) 
              ,@tick varchar(8) = '&#9989'
              ,@cross varchar(8) = '&#10060 '
              ,@TableStatus varchar(8) 
              ,@TableTemplate varchar(2000) = 
                      '<table class ="Table-format" Style="width: TableWidthpx;">             
                      <tr>          
                             <td class=Checkbox-format>TableCheck</style></td>
                             <td class=Title-format>Tabletitle</style></td>
                      </tr>
                      </table>'
              ,@Tablewidth int = 1000
              ,@ForceReport int =0
              ,@f_Emailimportance varchar(8) = 'High'
 
       DECLARE @HTML_All varchar(max), @HTML_Table varchar(max)
       DECLARE @Title varchar(1024)= @@servername+': SQL DBA - Daily Healthcheck Summary Report'
       --Open HTML
              SET @HTML_All ='<html>
              <head>
       <meta charset="UTF-8">
       <meta name="viewport" content="width=device-width, initial-scale=1.0">
       <style>
              .Table-format{border-collapse: collapse;border-top: 1px solid black;border-left: 1px solid black;font-family:Arial;border-bottom: none;border-right: none}
              .Checkbox-format {width: 20px;max-width: 20px;overflow-x: hidden;border-bottom: none;border-right: none;}
              .Title-format {overflow-x: hidden;font-weight:bold;border-bottom: none;border-right: none;width: 980px;max-width: 980px;}
       </style>
       <title>'+@Title+'</title>
       </head>
       <H1>'+@Title+'</H1>
       <body>'
       
 
IF NOT EXISTS (SELECT * from #backups)
       SELECT @TableStatus = @tick
       ELSE 
       SELECT @TableStatus = @cross
       IF @TableStatus = @cross
              SET @ForceReport = @ForceReport +1
       SELECT @HTML_All =@HTML_All +REPLACE(REPLACE(REPLACE(@TableTemplate,'TableWidth',@Tablewidth),'TableCheck',@TableStatus),'Tabletitle','Database Backup Latency')
       PRINT '#backups1'
       EXEC Convert2HTML @Table_name = '#backups1', @header ='None',@OrderBy = '1D'   ,@format = 2 ,@TableColor='Red',@f_body= @HTML_Table  output 
       IF @HTML_Table  IS NOT NULL 
              SELECT @HTML_All = @HTML_All +@HTML_Table 
       SELECT @HTML_All = @HTML_All +'<br>', @Tablewidth=@Tablewidth-100


IF NOT EXISTS (SELECT * from #JobFailures)
       SELECT @TableStatus = @tick
       ELSE 
       SELECT @TableStatus = @cross
       IF @TableStatus = @cross
              SET @ForceReport = @ForceReport +1
       SELECT @HTML_All =@HTML_All +REPLACE(REPLACE(REPLACE(@TableTemplate,'TableWidth',@Tablewidth),'TableCheck',@TableStatus),'Tabletitle','SQL Server Agent Job Failures')
       PRINT '#JobFailures'
       EXEC Convert2HTML @Table_name = '#JobFailures', @header ='None',@OrderBy = '1D'   ,@format = 2 ,@TableColor='Red',@f_body= @HTML_Table  output 
       IF @HTML_Table  IS NOT NULL 
              SELECT @HTML_All = @HTML_All +@HTML_Table 
       SELECT @HTML_All = @HTML_All +'<br>', @Tablewidth=@Tablewidth-100


 
IF NOT EXISTS (SELECT * from #LogBloat)
       SELECT @TableStatus = @tick
       ELSE 
       SELECT @TableStatus = @cross
--       IF @TableStatus = @cross
--              SET @ForceReport = @ForceReport +1
       SELECT @HTML_All =@HTML_All +REPLACE(REPLACE(REPLACE(@TableTemplate,'TableWidth',@Tablewidth),'TableCheck',@TableStatus),'Tabletitle','Transaction Log Bloat ')
       PRINT '#LogBloat'
       EXEC Convert2HTML @Table_name = '#LogBloat', @header ='None',@OrderBy = '1D'   ,@format = 2 ,@TableColor='Red',@f_body= @HTML_Table  output 
       IF @HTML_Table  IS NOT NULL 
              SELECT @HTML_All = @HTML_All +@HTML_Table 
       SELECT @HTML_All = @HTML_All +'<br>', @Tablewidth=@Tablewidth-100
 
IF NOT EXISTS (SELECT * from #StorageStatus)
       SELECT @TableStatus = @tick
       ELSE 
       SELECT @TableStatus = @cross
       IF @TableStatus = @cross
              SET @ForceReport = @ForceReport +1
       SELECT @HTML_All =@HTML_All +REPLACE(REPLACE(REPLACE(@TableTemplate,'TableWidth',@Tablewidth),'TableCheck',@TableStatus),'Tabletitle','Storage  Status')
       PRINT '#StorageStatus'
       EXEC Convert2HTML @Table_name = '#StorageStatus', @header ='None',@OrderBy = '1D'    ,@format = 2 ,@f_body= @HTML_Table  output 
       IF @HTML_Table  IS NOT NULL 
              SELECT @HTML_All = @HTML_All +@HTML_Table 
       SELECT @HTML_All = @HTML_All +'<br>', @Tablewidth=@Tablewidth-100
 
IF NOT EXISTS (SELECT * from #AGHealth)
       SELECT @TableStatus = @tick
       ELSE 
       SELECT @TableStatus = @cross
       
       IF @TableStatus = @cross
              SET @ForceReport = @ForceReport +1
 
       SELECT @HTML_All =@HTML_All +REPLACE(REPLACE(REPLACE(@TableTemplate,'TableWidth',@Tablewidth),'TableCheck',@TableStatus),'Tabletitle','Availability Group Health')
       IF @TableStatus = @tick
       BEGIN 
       PRINT '#AGHealth'
       EXEC Convert2HTML @Table_name = '#AGHealth', @header ='None',@OrderBy='1D',@format = 2 ,@TableColor='green',@f_body= @HTML_Table  output 
       END 
       ELSE
       BEGIN 
       PRINT '#AGHealth'
       EXEC Convert2HTML @Table_name = '#AGHealth', @header ='None',@OrderBy = '1D'      ,@format = 2 ,@TableColor='red',@f_body= @HTML_Table  output 
       END 
       IF @HTML_Table  IS NOT NULL 
              SELECT @HTML_All = @HTML_All +@HTML_Table 
       SELECT @HTML_All = @HTML_All +'<br>', @Tablewidth=@Tablewidth-100
 
IF NOT EXISTS (SELECT * from #AGJObSync)
       SELECT @TableStatus = @tick
       ELSE 
       SELECT @TableStatus = @cross
       
       IF @TableStatus = @cross
              SET @ForceReport = @ForceReport +1
                             
       SELECT @HTML_All =@HTML_All +REPLACE(REPLACE(REPLACE(@TableTemplate,'TableWidth',@Tablewidth),'TableCheck',@TableStatus),'Tabletitle','Availability Group Job Sync')
       PRINT '#AGJObSync'
       EXEC Convert2HTML @Table_name = '#AGJObSync', @header ='None',@OrderBy = '1D'    ,@format = 2 ,@f_body= @HTML_Table  output 

       IF @HTML_Table  IS NOT NULL 
              SELECT @HTML_All = @HTML_All +@HTML_Table 
       SELECT @HTML_All = @HTML_All +'<br>', @Tablewidth=@Tablewidth-100
 
IF NOT EXISTS (SELECT * from #AGLoginSync)
       SELECT @TableStatus = @tick
       ELSE 
       SELECT @TableStatus = @cross
       
       IF @TableStatus = @cross
              SET @ForceReport = @ForceReport +1
 
       SELECT @HTML_All =@HTML_All +REPLACE(REPLACE(REPLACE(@TableTemplate,'TableWidth',@Tablewidth),'TableCheck',@TableStatus),'Tabletitle','Availability Group Login')
       PRINT '#AGLoginSync'
       EXEC Convert2HTML @Table_name = '#AGLoginSync', @header ='None',@OrderBy = '1D'       ,@format = 2 ,@f_body= @HTML_Table  output 

       IF @HTML_Table  IS NOT NULL 
              SELECT @HTML_All = @HTML_All +@HTML_Table 
       SELECT @HTML_All = @HTML_All +'<br>', @Tablewidth=@Tablewidth-50
 
IF NOT EXISTS (SELECT * from #AGFailover)
       SELECT @TableStatus = @tick
       ELSE 
       SELECT @TableStatus = @cross
 
       IF @TableStatus = @cross
              SET @ForceReport = @ForceReport +1
 
       SELECT @HTML_All =@HTML_All +REPLACE(REPLACE(REPLACE(@TableTemplate,'TableWidth',@Tablewidth),'TableCheck',@TableStatus),'Tabletitle','Availability Failover')
       PRINT '#AGFailover'
       EXEC Convert2HTML @Table_name = '#AGFailover', @header ='None',@OrderBy = '1D'     ,@format = 2 ,@f_body= @HTML_Table  output 

       IF @HTML_Table  IS NOT NULL 
              SELECT @HTML_All = @HTML_All +@HTML_Table 
       SELECT @HTML_All = @HTML_All +'<br>', @Tablewidth=@Tablewidth-50
 
IF NOT EXISTS (SELECT * from #SQLrestart)
       SELECT @TableStatus = @tick
       ELSE 
       SELECT @TableStatus = @cross
 
       IF @TableStatus = @cross
              SET @ForceReport = @ForceReport +1
 
       SELECT @HTML_All =@HTML_All +REPLACE(REPLACE(REPLACE(@TableTemplate,'TableWidth',@Tablewidth),'TableCheck',@TableStatus),'Tabletitle','Last SQL restart')
       PRINT '#SQLrestart'
       EXEC Convert2HTML @Table_name = '#SQLrestart', @header ='None',@OrderBy = '1D'       ,@format = 2 ,@f_body= @HTML_Table  output 
		

       IF @HTML_Table  IS NOT NULL 
              SELECT @HTML_All = @HTML_All +@HTML_Table 
       SELECT @HTML_All = @HTML_All +'<br>', @Tablewidth=@Tablewidth-50
 
 


 
-- Close the HTML 
SELECT @HTML_All = @HTML_All +'</body></html>'
       
--Copy and paste this into a html file and open in browser or send as HTML email 
--            PRINT @HTML_All   
 
IF @ForceReport>0
SET @f_Emailimportance ='High'
ELSE
SET @f_Emailimportance ='Normal'
 
/* 
EXEC msdb.dbo.sp_send_dbmail
       @body = @HTML_All,
       @body_format ='HTML',
       @recipients = @EmailTo , 
       @subject = @Title ,
       @importance =  @f_Emailimportance 
 */


SELECT * from #JobFailures
SELECT * from #Backups1
SELECT * from #LogBloat
SELECT * from #StorageStatus
SELECT * from #AGHealth
SELECT * from #AGJObSync
SELECT * from #AGLoginSync
SELECT * from #AGFailover
SELECT * from #SQLrestart

SELECT @HTML_All EmailBody into #q

select @@servername SQLInstance , EmailBody  , getdate() CaptureDT from #q

--Figure SUM(Max(Column Length)

DECLARE @SQL_Query nvarchar(max) ='SELECT MAX(',  @TableName nvarchar(128), @TableLength int
SELECT @SQL_Query = 'SELECT MAX('
SELECT TOP 1 @TableName = name from tempdb..sysobjects  where name like '#JobFailures%' ORDER BY crdate DESC
SELECT @SQL_Query = @SQL_Query +' LEN('+Column_Name+') +' FROM tempdb.INFORMATION_SCHEMA.Columns WHERE TABLE_NAME  = @TableName 
SELECT @SQL_Query = LEFT (@SQL_Query, LEN(@SQL_Query ) -1)
SELECT @SQL_Query = @SQL_Query +') FROM '+ @TableName 
EXEC sp_executesql  @SQL_Query , N'@outValue int output', @Outvalue = @TableLength  OUTPUT 
print @TableLength  