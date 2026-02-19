;WITH CTE AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY name) AS rn,
        COUNT(*) OVER () AS total,
        name
    FROM sys.configurations
)
SELECT 
    'SELECT ''' 
    + name 
    + ''' AS configuration_name, value, value_in_use FROM sys.configurations WHERE name = ''' 
    + name 
    + ''''
    + CASE WHEN rn < total THEN CHAR(10) + 'UNION ALL' ELSE '' END
FROM CTE
ORDER BY rn;
