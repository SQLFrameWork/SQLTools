DECLARE @NewLocation NVARCHAR(260) = N'xxx:\';
DECLARE @SQL NVARCHAR(MAX) = N'';

SELECT @SQL = @SQL +
    'ALTER DATABASE tempdb MODIFY FILE (NAME = ' + QUOTENAME(f.name,'''') +
    ', FILENAME = ''' + @NewLocation +
    RIGHT(f.physical_name, CHARINDEX('\', REVERSE(f.physical_name)) - 1) + ''');' + CHAR(13)
FROM sys.master_files f
WHERE f.database_id = DB_ID('tempdb');

PRINT @SQL;
