CREATE OR ALTER PROC Test_CUD

    @DropCache varchar (8)  = NULL 
    ,@DatabaseName varchar(128)= NULL 
    ,@Iterations int = 10000    


AS
BEGIN
/*
### Procedure: Test_CUD
**Author:**       Mark.Rollinson@SQLFramework.com  
**Create date:**  10/08/2022
**Notes:**  
- Run a few inserts / updates / deletes against a table
  - This is NOT a scientific test of anything ! 
  - Just a sanity check to baseline i/o performance 
**Parameters :**  
    @DropCache                  Enter any string value to clear buffer and plan caches prior to run 
    @DatabaseName               Specify which database to use 
                                 - If NULL then current database is used 
                                 - If database specified exists then test table is added and dropped 
                                 - If database specified does not exist then the database is created and then dropped 
    @Iterations                  - Number of rows created , Inserted or deleted during the test  (nothing speical about the default) 
*/

EXEC('DROP TABLE IF EXISTS [' + @DatabaseName + ']..TestTable')



IF @DropCache  IS NOT NULL
BEGIN 
EXEC ('DBCC DROPCLEANBUFFERS')
EXEC ('DBCC FREEPROCCACHE')
END 

DECLARE @StartTime datetime2, @EndTime datetime2, @Step varchar(100), @SQL_Query nvarchar(MAX) , @NewDB int 
        
-- Create database if applicable 
CREATE TABLE #Timings (Step nvarchar(100),Duration_ms bigint)

IF @DatabaseName IS NULL
    SET @DatabaseName  = DB_name()

IF NOT EXISTS (SELECT 1 from sys.databases where name = @DatabaseName  )
SELECT @NewDB = 1
ELSE 
SELECT @NewDB = 0

IF @NewDB = 1
SELECT  @Step = 'Create Database: '+@DatabaseName
        ,@StartTime = sysdatetime() -- for extra precision 
ELSE 
SELECT  @Step = 'Using Database: '+@DatabaseName
        ,@StartTime = sysdatetime() -- for extra precision 


IF NOT EXISTS (SELECT 1 from sys.databases where name = @DatabaseName  )
    EXEC('CREATE DATABASE [' + @DatabaseName + ']')

SELECT @EndTime = sysdatetime()

INSERT INTO #Timings VALUES (@Step, DATEDIFF(ms, @StartTime, @EndTime))

SELECT  @SQL_Query = 'EXEC(''CREATE TABLE ' + @DatabaseName + '.dbo.TestTable (ID int PRIMARY KEY, Data1 nvarchar(100),Data2 nvarchar(100), CreatedAt datetime2)'')'

EXEC(@SQL_Query)

-- Insert rows - single insert
SELECT @Step = 'Insert '+cast(@Iterations as varchar(8))  +' Rows - Single Insert'
       ,@StartTime = sysdatetime()

SELECT @SQL_Query = '
                    DECLARE @i INT = 1;
                    WHILE @i <= '+cast(@Iterations as varchar(8))  +' 
                    BEGIN
                        INSERT INTO [' + @DatabaseName + '].dbo.TestTable (ID, Data1, Data2, CreatedAt)
                        VALUES (@i, REPLICATE(''A'', 50), REPLICATE(''B'', 50), sysdatetime());
                        SET @i += 1;
                    END
                    '
EXEC(@SQL_Query)

SELECT @EndTime = sysdatetime()
INSERT INTO #Timings VALUES (@Step, DATEDIFF(ms, @StartTime, @EndTime));

-- Insert rows - batch insert
SELECT @Step = 'Insert '+cast(@Iterations as varchar(8))  +'  Rows - Batch Insert'
        ,@StartTime = sysdatetime()

SELECT @SQL_Query = '
    USE [' + @DatabaseName + '];
    INSERT TestTable (ID, Data1, Data2, CreatedAt)
    SELECT TOP '+cast(@Iterations as varchar(8))  +' 
        '+cast(@Iterations as varchar(8))  +'  + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ID,
        REPLICATE(''C'', 50),
        REPLICATE(''D'', 50),
        sysdatetime()
    FROM sys.all_objects a CROSS JOIN sys.all_objects b
'
EXEC(@SQL_Query);

SELECT @EndTime = sysdatetime()
INSERT INTO #Timings VALUES (@Step, DATEDIFF(ms, @StartTime, @EndTime));

-- Rebuild Index
SELECT @Step = 'Rebuild Index'
        ,@StartTime = sysdatetime()
        , @SQL_Query = '
            USE [' + @DatabaseName + '];
            ALTER INDEX ALL ON dbo.TestTable REBUILD;
            '
EXEC(@SQL_Query)

SELECT @EndTime = sysdatetime()
INSERT INTO #Timings VALUES (@Step, DATEDIFF(ms, @StartTime, @EndTime));

-- Update rows - single update
SELECT @Step = 'Update '+cast(@Iterations as varchar(8))  +'  Rows - Single Update'
       ,@StartTime = sysdatetime()
       ,@SQL_Query = '
            USE [' + @DatabaseName + '];
            DECLARE @i INT = 1;
            WHILE @i <= '+cast(@Iterations as varchar(8))  +' 
            BEGIN
                UPDATE dbo.TestTable
                SET Data1 = REPLICATE(''X'', 50)
                WHERE ID = @i;
                SET @i += 1;
            END
            '
EXEC(@SQL_Query)

SELECT @EndTime = sysdatetime()
INSERT INTO #Timings VALUES (@Step, DATEDIFF(ms, @StartTime, @EndTime))

-- Update  rows - batch update
SELECT @Step = 'Update '+cast(@Iterations as varchar(8))  +' Rows - Batch Update'
        ,@StartTime = sysdatetime()
        ,@SQL_Query = '
            USE [' + @DatabaseName + '];
            UPDATE dbo.TestTable
            SET Data2 = REPLICATE(''Y'', 50)
            WHERE ID BETWEEN '+cast(@Iterations+1 as varchar(8))  +'  AND '+cast(@Iterations*2 as varchar(8))  +' ;
            '
EXEC(@SQL_Query)

SELECT @EndTime = sysdatetime()
INSERT INTO #Timings VALUES (@Step, DATEDIFF(ms, @StartTime, @EndTime))

-- Rebuild Index again
SELECT @Step = 'Rebuild Index After Updates'
        ,@StartTime = sysdatetime()
        ,@SQL_Query = '
            USE [' + @DatabaseName + '];
            ALTER INDEX ALL ON dbo.TestTable REBUILD;
            '
EXEC(@SQL_Query)

SELECT @EndTime = sysdatetime()
INSERT INTO #Timings VALUES (@Step, DATEDIFF(ms, @StartTime, @EndTime))

-- Delete 1/2 rows - single delete
SELECT  @Step = 'Delete '+cast(@Iterations/2 as varchar(8))  +' Rows - Single Delete'
        ,@StartTime = sysdatetime()
        ,@SQL_Query = '
            USE [' + @DatabaseName + '];
            DECLARE @i INT = 1;
            WHILE @i <= '+cast(@Iterations/2 as varchar(8))  +' 
            BEGIN
                DELETE FROM dbo.TestTable WHERE ID = @i;
                SET @i += 1;
            END
            '
EXEC(@SQL_Query)

SELECT @EndTime = sysdatetime()
INSERT INTO #Timings VALUES (@Step, DATEDIFF(ms, @StartTime, @EndTime))

-- Delete 1/2 rows - batch delete
SELECT @Step = 'Delete '+cast(@Iterations/2 as varchar(8))  +'  Rows - Batch Delete'
        ,@StartTime = sysdatetime()
        ,@SQL_Query = '
            USE [' + @DatabaseName + '];
            DELETE FROM dbo.TestTable WHERE ID BETWEEN '+cast(@Iterations+1 as varchar(8))  +' AND '+cast(@Iterations+(@Iterations /2)  as varchar(8))  +' ;
            '
EXEC(@SQL_Query)

SELECT @EndTime = sysdatetime()
INSERT INTO #Timings VALUES (@Step, DATEDIFF(ms, @StartTime, @EndTime))

-- Final output
SELECT * , Getdate() RunDT, SERVERPROPERTY('MachineName') HostName
FROM #Timings;

IF @NewDB = 1
EXEC('DROP DATABASE [' + @DatabaseName + ']')

DROP TABLE #Timings

EXEC('DROP TABLE IF EXISTS [' + @DatabaseName + ']..TestTable')

END 
GO 

EXEC Test_CUD @databasename ='SQLToolKit'
