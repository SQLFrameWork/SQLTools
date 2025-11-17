DROP TABLE IF EXISTS #Command 

CREATE TABLE #Command (Query varchar(max))

DROP TABLE IF EXISTS #Results 

CREATE TABLE #Results  (DBName varchar(128) , DBSchema varchar(128) ,TableName varchar(128) ,ColumnName varchar(128) , MaxDate datetime)

EXEC sp_msforeachdb 'INSERT #Command (Query)

select ''SELECT 

''''''+TABLE_CATALOG+'''''' DBname , ''''''+TABLE_SCHEMA+''''''  DBSchema, ''''''+TABLE_NAME+'''''' TableName, 

''''''+Column_name+'''''' , MAX(''+quotename(Column_name)+'') 
from	''+quotename(TABLE_CATALOG) +''.''+quotename(TABLE_SCHEMA) +''.''+quotename(TABLE_NAME) +''

WHERE	''+quotename(Column_name)+'' IS NOT NULL 
HAVING MAX(''+quotename(Column_name)+'') IS NOT NULL''



from [?].INFORMATION_SCHEMA.columns
WHERE data_type like ''date%''
AND TABLE_NAME in (

select name from [?]..sysobjects where id in 
(select id  from [?]..sysindexes
where rows > 1000
)
and xtype = ''U''

)

'

DECLARE @sqlquery varchar(max) =''

WHILE EXISTS (select TOP 1 * from #Command)
BEGIN 
SET @sqlquery = ''
select TOP 1 @sqlquery = query from #Command  
ORDER BY Query

PRINT @sqlquery 
INSERT #Results
EXEC (@sqlquery )



DELETE #Command   WHERE @sqlquery = query 

END 


;WITH CTE AS (
select * 
, ROW_NUMBER() OVER(PARTITION BY DBname ORDER BY MaxDate DESC) Row#
from #Results
)

SELECT DBName	,DBSchema	,TableName	,ColumnName	,MaxDate	FROM CTE WHERE Row#<=1




