SELECT
    @@SERVERNAME SQLinstance, 
    sp.name login_name,
    sp.sid,
    CONVERT(varchar(256), sl.password_hash, 1) AS #hash
    ,NULL [LastAction]
    ,getdate() lastChangeDate
FROM sys.server_principals  sp
LEFT JOIN sys.sql_logins    sl  ON sp.principal_id = sl.principal_id
WHERE sp.type_desc in ('SQL_LOGIN','WINDOWS_LOGIN')
    AND sp.name not like 'NT_%'
    AND sp.name not like '##%'
ORDER BY sp.type_desc, sp.name;


/****** Object:  StoredProcedure [dbo].[Get_AuditHistory]    Script Date: 15/10/2025 12:01:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   OR ALTER PROCEDURE [dbo].[Get_SysLogins]
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

drop table if exists #CurrentLogins
SELECT
    @@SERVERNAME SQLinstance, 
    sp.name login_name,
    sp.sid,
    ISNULL(sl.password_hash, 0x00) PWHash
    INTO #CurrentLogins
FROM sys.server_principals  sp
LEFT JOIN sys.sql_logins    sl  ON sp.principal_id = sl.principal_id
WHERE sp.type_desc in ('SQL_LOGIN','WINDOWS_LOGIN')
    AND sp.name not like 'NT_%'
    AND sp.name not like '##%'
ORDER BY sp.type_desc, sp.name;

-- UPDATE Changes to existing Job Steps 

UPDATE t
SET 
    t.sid= s.sid
    ,t.PWHash= s.PWHash
    ,t.LastAction = 'UPDATE'
    ,t.LastChangeDate = getdate()
FROM Logins t
JOIN #CurrentLogins s ON t.SQLInstance  = s.SQLInstance 
                    AND t.Login_name = s.Login_name
WHERE   t.sid <> s.sid 
    OR  t.PWHash<> s.PWHash

-- INSERT New Logins 

INSERT INTO Logins 
SELECT * 
    ,'INSERT' 
    ,getdate() 
FROM #CurrentLogins   s
WHERE NOT EXISTS (
    SELECT 1 
    FROM dbo.Logins t
    WHERE t.SQLInstance = s.SQLInstance
      AND t.login_name  = s.login_name 

);

-- Flag DELETED jobsteps 

UPDATE t
SET 
    t.LastAction = 'DELETED'
    ,t.LastChangeDate = getdate()
FROM dbo.Logins  t
WHERE NOT EXISTS (
    SELECT 1
    FROM #CurrentLogins s
    WHERE t.SQLInstance = s.SQLInstance
      AND t.Login_name  = s.Login_name 
);


END 

GO 


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   OR ALTER PROCEDURE [dbo].[Rpt_SysLogins]
AS
BEGIN

/*
### Procedure: Rpt_SysJobs
**Author:**       Mark.Rollinson@SQLFramework.com  
**Create date:**  13/08/2024  
**Notes:**  
- Report any out of sync job steps by comparing msdb..SysJobs  to local SysJobs table 
*/



DROP TABLE IF EXISTS #CurrentLogins 

SELECT
    @@SERVERNAME SQLinstance, 
    sp.name login_name,
    sp.sid,
    ISNULL(sl.password_hash,0x00) PWHash
    ,NULL [LastAction]
    ,getdate() lastChangeDate
    INTO #CurrentLogins
FROM sys.server_principals  sp
LEFT JOIN sys.sql_logins    sl  ON sp.principal_id = sl.principal_id
WHERE sp.type_desc in ('SQL_LOGIN','WINDOWS_LOGIN')
    AND sp.name not like 'NT_%'
    AND sp.name not like '##%'


SELECT 
    ISNULL(l.SQLInstance, t.SQLInstance) SQLInstance
    ,ISNULL(l.Login_name, t.Login_name)       LoginHame
    ,CASE WHEN ISNULL(l.sid,0x00)<>ISNULL(t.sid,0x01)  THEN 'OutOfSync' 
    --when t.Login_name='DELETED' then 'Deleted On Primary'
    Else 'ok' END SID_Status
    ,CASE WHEN ISNULL(l.PWhash,0x01) <>ISNULL(t.PWhash,0x01)  THEN 'OutOfSync' Else 'ok' END Password_Status 
FROM #CurrentLogins l
FULL OUTER JOIN dbo.Logins t
    ON l.SQLInstance = t.SQLInstance
    AND l.Login_name= t.Login_name
    and t.LastAction!='DELETED'
WHERE
    (   l.Login_name IS NULL            -- exists in table but not in msdb
        OR t.Login_name IS NULL        -- exists in msdb but not in table
        OR l.sid<> t.sid 
        OR l.PWhash<> t.PWhash
    )
    and ISNULL(t.LastAction,'')!='DELETED'

END 

-- IF PRIMARY 
IF EXISTS ( SELECT sys.fn_hadr_is_primary_replica(name) FROM sys.databases 
            WHERE replica_id IS NOT Null and sys.fn_hadr_is_primary_replica(name) =1 )
EXEC Get_SysLogins 

ELSE IF EXISTS ( SELECT sys.fn_hadr_is_primary_replica(name) FROM sys.databases 
            WHERE replica_id IS NOT Null and sys.fn_hadr_is_primary_replica(name) =0 )
EXEC Rpt_SysLogins 

ELSE IF NOT EXISTS (SELECT sys.fn_hadr_is_primary_replica(name)    FROM sys.databases WHERE replica_id IS NOT Null)
PRINT 'Do Nothing' 


