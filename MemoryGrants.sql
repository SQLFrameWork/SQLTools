
SELECT
    mg.session_id,
    mg.request_time,
    mg.grant_time,
    mg.requested_memory_kb / 1024.0 AS RequestedMB,
    mg.granted_memory_kb   / 1024.0 AS GrantedMB,
    mg.used_memory_kb      / 1024.0 AS UsedMB,
    mg.max_used_memory_kb  / 1024.0 AS MaxUsedMB,
    mg.dop,
    mg.queue_id,
    mg.resource_semaphore_id,
    DB_NAME(er.database_id) AS DatabaseName,
    er.status,
    er.command,
    er.wait_type,
    er.cpu_time,
    er.total_elapsed_time,
    st.text AS SqlText
FROM sys.dm_exec_query_memory_grants AS mg
LEFT JOIN sys.dm_exec_requests AS er
    ON mg.session_id = er.session_id
OUTER APPLY sys.dm_exec_sql_text(er.sql_handle) AS st
WHERE mg.request_time IS NOT NULL
AND er.session_id IS NOT NULL
AND mg.granted_memory_kb > 0

ORDER BY GrantedMB DESC;



SELECT
    t.session_id,
    DB_NAME(s.database_id) AS DatabaseName,
    CAST((t.user_objects_alloc_page_count 
        + t.internal_objects_alloc_page_count) / 128.0 AS DECIMAL(10,2)) AS TempdbMB,
    CAST(t.user_objects_alloc_page_count / 128.0 AS DECIMAL(10,2))        AS UserObjectsMB,
    CAST(t.internal_objects_alloc_page_count / 128.0 AS DECIMAL(10,2))    AS InternalObjectsMB,
    s.login_name,
    s.host_name,
    s.program_name,
    r.status,
    r.command,
    r.cpu_time,
    r.total_elapsed_time,
    SUBSTRING(st.text, (r.statement_start_offset/2) + 1,
                     ((CASE r.statement_end_offset 
                           WHEN -1 THEN DATALENGTH(st.text)
                           ELSE r.statement_end_offset
                       END - r.statement_start_offset)/2) + 1) AS StatementText
FROM sys.dm_db_task_space_usage AS t
JOIN sys.dm_exec_sessions AS s
    ON t.session_id = s.session_id
JOIN sys.dm_exec_requests AS r
    ON t.session_id = r.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
WHERE t.internal_objects_alloc_page_count + t.user_objects_alloc_page_count > 0
ORDER BY TempdbMB DESC;
