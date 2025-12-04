USE [DBA_Admin]
GO

/****** Object:  StoredProcedure [dbo].[ReOrg_PartitionedTables]    Script Date: 02/12/2025 13:27:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[ReOrg_PartitionedTables]
(
      @DatabaseName SYSNAME
    , @SchemaName   SYSNAME
    , @TableName    SYSNAME
      , @FragLevel int = 5
      ,@Debug int =0

)
AS
BEGIN
    SET NOCOUNT ON

      /*
### Procedure: ReOrg_PartitionedTables
**Author:**       Mark.Rollinson@SQLFramework.com  
**Create date:**  22/11/2025
**Notes:**  
  - Re-organizes and updates stats on table as declared above where fragmentation on that partition is > @FragLevel
  - This resolves a specific problem with Tiger_Prism_Tenant_FHFT_Warehouse where some tables have multiple partitions written to concurrently during index maintenance
  - This causes deadlocks on those tables so they have been excluded in the main Ola Maintenance run and replaced by this
  -  Write maintatance results to Ola H CommandLog
**Parameters :**  
    @DatabaseName               Specify which database to use
    @SchemaName                 Schema
    @TableName                  Table (checks all indexes in that table)
    @FragLevel                  Defaults to 5%
      @Debug =1                           Just prints to screen without applying
   
*/

    ---------------------------------------------------------------------
    -- Validate Database Exists, is PRIMARY if in an AG and ONLINE
    ---------------------------------------------------------------------
    IF EXISTS (
        SELECT 1
        FROM sys.databases
        WHERE name = @DatabaseName AND replica_id IS NOT NULL AND state_desc = 'ONLINE'
    )
    BEGIN
        IF sys.fn_hadr_is_primary_replica(@DatabaseName) = 0
        BEGIN
            RAISERROR('Database %s Not in Primary Replica for this opertation.', 16, 1, @DatabaseName)
            RETURN
        END
ELSE
    IF NOT  EXISTS (
        SELECT 1
        FROM sys.databases
        WHERE name = @DatabaseName AND replica_id IS NOT NULL AND state_desc = 'ONLINE'
    )

        BEGIN
            RAISERROR('Database %s is unavailable for this opertation.', 16, 1, @DatabaseName)
            RETURN
        END

    END

    ---------------------------------------------------------------------
    -- Build full table name and verify it exists
    ---------------------------------------------------------------------
DECLARE @TableExists BIT = 0
DECLARE @sql NVARCHAR(MAX)

SET @sql = '
    SELECT @TableExists_OUT = CASE
        WHEN EXISTS (
            SELECT 1
            FROM ' + QUOTENAME(@DatabaseName) + '.sys.objects
            WHERE object_id = OBJECT_ID('''
                + QUOTENAME(@DatabaseName) + '.'
                + QUOTENAME(@SchemaName) + '.'
                + QUOTENAME(@TableName) + ''')
        ) THEN 1 ELSE 0 END
'

EXEC sp_executesql
    @sql,
    N'@TableExists_OUT BIT OUTPUT',
    @TableExists_OUT = @TableExists OUTPUT

IF @TableExists = 0
BEGIN
    RAISERROR('Table %s.%s.%s does not exist in database %s.',
              16, 1, @SchemaName, @TableName, @DatabaseName, @DatabaseName)
    RETURN
END

    ---------------------------------------------------------------------
    -- Temp table dor frag results
    ---------------------------------------------------------------------
    DROP TABLE IF EXISTS #frag

    CREATE TABLE #frag
    (
          object_id INT
        , index_id INT
        , partition_number INT
        , avg_fragmentation_in_percent FLOAT
        , page_count BIGINT
    )

    ---------------------------------------------------------------------
    -- Dynamic SQL for sys.dm_db_index_physical_stats
    ---------------------------------------------------------------------
DECLARE @FullObjectName NVARCHAR(400) =
        QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)

    SET @sql = '
        INSERT INTO #frag(object_id, index_id, partition_number, avg_fragmentation_in_percent, page_count)
        SELECT
              object_id
            , index_id
            , partition_number
            , avg_fragmentation_in_percent
            , page_count
        FROM ' + QUOTENAME(@DatabaseName) + '.sys.dm_db_index_physical_stats(
              DB_ID(''' + @DatabaseName + '''),
              OBJECT_ID(''' + @FullObjectName + '''),
              NULL,
              NULL,
              ''LIMITED''
        )
    '
    EXEC(@sql)

    ---------------------------------------------------------------------
    -- Loop partitions
    ---------------------------------------------------------------------
    DECLARE
          @index_id INT
        , @partition INT
        , @frag FLOAT
        , @index_name SYSNAME
        , @cmd NVARCHAR(MAX)
        , @startTime DATETIME
        , @endTime DATETIME
        , @error INT
        , @errmsg NVARCHAR(4000)
        , @cmdType SYSNAME

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT index_id, partition_number, avg_fragmentation_in_percent
    FROM #frag
    WHERE page_count > 100
    ORDER BY index_id, partition_number

    OPEN cur
    FETCH NEXT FROM cur INTO @index_id, @partition, @frag

    WHILE @@FETCH_STATUS = 0
    BEGIN
---------------------------------------------------------------------
-- Get index name from the TARGET database
---------------------------------------------------------------------
DECLARE @sqlGetIndexName NVARCHAR(MAX), @idx_out SYSNAME

SET @sqlGetIndexName = '
    SELECT @idx_out = i.name
    FROM ' + QUOTENAME(@DatabaseName) + '.sys.indexes i
    WHERE i.object_id = OBJECT_ID(''' + @FullObjectName + ''')
      AND i.index_id = ' + CAST(@index_id AS NVARCHAR(10)) + '
'

EXEC sp_executesql
      @sqlGetIndexName,
      N'@idx_out SYSNAME OUTPUT',
      @idx_out=@idx_out OUTPUT

SET @index_name = @idx_out

        IF @frag > @FragLevel
        BEGIN
            ------------------------------------------------------
            -- REORGANIZE PARTITION
            ------------------------------------------------------
            SET @cmd = N'ALTER INDEX ' + QUOTENAME(@index_name) +
                       N' ON ' + @FullObjectName +
                       N' REORGANIZE PARTITION = ' + CAST(@partition AS NVARCHAR(12)) + N''

            SET @cmdType = 'INDEX_REORGANIZE'
            SET @startTime = GETDATE()
            BEGIN TRY
                If @Debug = 1
                        PRINT @cmd
                        ELSE
                        BEGIN
                        EXEC(@cmd)
                SET @error = 0
                SET @errmsg = NULL
                        END
            END TRY
            BEGIN CATCH
                SET @error = ERROR_NUMBER()
                SET @errmsg = ERROR_MESSAGE()
            END CATCH
            SET @endTime = GETDATE()
                  
                  IF ISNULL(@Debug,0) = 1
            INSERT INTO dbo.CommandLog
            (
                DatabaseName, SchemaName, ObjectName, IndexName, PartitionNumber,
                Command, CommandType, StartTime, EndTime, ErrorNumber, ErrorMessage
            )
            VALUES
            (
                @DatabaseName, @SchemaName, @TableName, @index_name, @partition,
                @cmd, @cmdType, @startTime, @endTime, @error, @errmsg
            )
                  
            ------------------------------------------------------
            -- UPDATE STATISTICS FOR PARTITION
            ------------------------------------------------------
DECLARE @IsColumnstore BIT = 0
DECLARE @sqlCheckColstore NVARCHAR(MAX)

SET @sqlCheckColstore = '
    SELECT @IsColumnstore_OUT = CASE WHEN EXISTS
    (
        SELECT 1
        FROM ' + QUOTENAME(@DatabaseName) + '.sys.indexes i
        WHERE i.object_id = OBJECT_ID(''' + @FullObjectName + ''')
          AND i.index_id = ' + CAST(@index_id AS NVARCHAR(10)) + '
          AND i.type IN (5,6)
    ) THEN 1 ELSE 0 END
'

EXEC sys.sp_executesql
        @sqlCheckColstore,
        N'@IsColumnstore_OUT BIT OUTPUT',
        @IsColumnstore_OUT = @IsColumnstore OUTPUT

----------------------------------------------------------------------
-- Skip UPDATE STATISTICS for columnstore indexes
----------------------------------------------------------------------

IF @IsColumnstore = 1
BEGIN
    PRINT 'Skipping UPDATE STATISTICS for columnstore index: ' + @index_name
      IF ISNULL(@Debug,0) = 1
    INSERT INTO dbo.CommandLog
    (
        DatabaseName, SchemaName, ObjectName, IndexName, PartitionNumber,
        Command, CommandType, StartTime, EndTime, ErrorNumber, ErrorMessage
    )
    VALUES
    (
        @DatabaseName, @SchemaName, @TableName, @index_name, @partition,
        'Skipped UPDATE STATISTICS due to columnstore index',
        'UPDATE_STATISTICS_SKIPPED',
        GETDATE(), GETDATE(), 0, NULL
    )

END
ELSE

BEGIN
            SET @cmd = N'UPDATE STATISTICS ' + QUOTENAME(@DatabaseName) + N'.' +
                       QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName) + N' ' +
                       QUOTENAME(@index_name) + N' WITH RESAMPLE ON PARTITIONS (' +
                       CAST(@partition AS NVARCHAR(12)) + N')'

            SET @cmdType = 'UPDATE_STATISTICS'
            SET @startTime = GETDATE()
            BEGIN TRY
                  If @Debug = 1
                  PRINT @cmd
                  ELSE
                  BEGIN
              EXEC(@cmd)
              SET @error = 0
              SET @errmsg = NULL
                  END
            END TRY
            BEGIN CATCH
                SET @error = ERROR_NUMBER()
                SET @errmsg = ERROR_MESSAGE()
            END CATCH
            SET @endTime = GETDATE()
                  IF ISNULL(@Debug,0) = 1
            INSERT INTO dbo.CommandLog
            (
                DatabaseName, SchemaName, ObjectName, IndexName, PartitionNumber,
                Command, CommandType, StartTime, EndTime, ErrorNumber, ErrorMessage
            )
            VALUES
            (
                @DatabaseName, @SchemaName, @TableName, @index_name, @partition,
                @cmd, @cmdType, @startTime, @endTime, @error, @errmsg
            )
                  
        END
            END
        FETCH NEXT FROM cur INTO @index_id, @partition, @frag
    END

    CLOSE cur
    DEALLOCATE cur

END

GO

