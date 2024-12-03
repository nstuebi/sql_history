/****** Script for SelectTopNRows command from SSMS  ******/
with tables_in_scope as (
select 'schemaname' Scheme, 'tabellenname' Tbl union
select 'schemaname' Scheme, 'tabellenname2' Tbl 

)
, additional_columns as (
select 'hst_uuid' column_name, 0 Ordinal_position, '(newid())' Column_Default, 'NO' IS_Nullable, '[uniqueidentifier]' DataType union
select 'sqlAction' column_name, 1000 Ordinal_position, null Column_Default, 'NO' IS_Nullable, '[char](1)' DataType union
select 'validFrom' column_name, 1001 Ordinal_position, null Column_Default, 'NO' IS_Nullable, '[datetime2](7)' DataType union
select 'validTo' column_name, 1002 Ordinal_position, null Column_Default, 'YES' IS_Nullable, '[datetime2](7)' DataType 
)

, table_columns_hst as (
SELECT [TABLE_SCHEMA]
      ,[TABLE_NAME]
      ,[COLUMN_NAME]
      ,[ORDINAL_POSITION]
      ,[COLUMN_DEFAULT]
      ,[IS_NULLABLE]

	 , case when [CHARACTER_MAXIMUM_LENGTH] is null then'['+[DATA_TYPE]+ ']'
			when [DATA_TYPE]= 'nvarchar' and [CHARACTER_MAXIMUM_LENGTH] = -1 then '['+ [DATA_TYPE]  + ']'+'(max)'
			when [DATA_TYPE]= 'varchar' and [CHARACTER_MAXIMUM_LENGTH] = -1 then '['+ [DATA_TYPE]+ ']'  + '(max)'
			when [DATA_TYPE]= 'varbinary' and [CHARACTER_MAXIMUM_LENGTH] = -1 then'['+  [DATA_TYPE]+ ']'  + '(max)'
			else '['+[DATA_TYPE]+ ']'  + '(' + Convert(varchar(200),[CHARACTER_MAXIMUM_LENGTH]) + ')' end as DataType
  FROM [INFORMATION_SCHEMA].[COLUMNS] c
  join tables_in_scope ts on ts.Scheme=c.table_schema and ts.Tbl = c.TABLE_NAME

  union all
  Select ts.Scheme [TABLE_SCHEMA], ts.Tbl TABLE_NAME,ac.column_name, ac.Ordinal_position, ac.Column_Default, ac.IS_Nullable, ac.DataType
  from additional_columns ac cross join tables_in_scope ts
  )
 , column_definitions as ( 
  select table_schema,
    table_name,
	ORDINAL_POSITION, 
	'[' + COLUMN_NAME +'] '+DataType + CASE WHEN COLUMN_DEFAULT is not null then ' DEFAULT ' + COLUMN_DEFAULT ELSE '' END + CASE WHEN [IS_NULLABLE] = 'NO' Then ' NOT' ELSE '' END + ' NULL' col_definition
  from table_columns_hst 
  --order by TABLE_SCHEMA,table_name,ORDINAL_POSITION 
 
 )

 select table_schema,
     table_name,
    'CREATE TABLE [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '_HST]( ' + CHAR(13) + '  '+  --Start
    STRING_AGG(col_definition,',' + CHAR(13) + '  ') WITHIN GROUP (ORDER BY ordinal_position ASC) + --Columns
	CHAR(13)+' , CONSTRAINT [PK_' + TABLE_NAME + '_HST] PRIMARY KEY CLUSTERED 
 (
	[hst_uuid] ASC
 )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
 ) ON [PRIMARY] 
GO' HST_Table_create_statement



 from column_definitions group by table_schema,table_name

 -- where data_type not in ('int','nvarchar','tinyint','numeric','datetime','varchar','bit','date','bigint','nchar','smallint')
