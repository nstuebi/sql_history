/****** Script for SelectTopNRows command from SSMS  ******/
with tables_in_scope as (
select 'schemaname' Scheme, 'table_name1' Tbl union
select 'schemaname' Scheme, 'table_name2' Tbl 
),

primary_keys as (

  select schema_name(tab.schema_id) as [schema_name], 
    pk.[name] as pk_name,
    ic.index_column_id as column_id,
    col.[name] as column_name, 
    tab.[name] as table_name,
	  case when c.[CHARACTER_MAXIMUM_LENGTH] is null then '['+ col.[name] + '] ['+c.[DATA_TYPE]+ ']'
			when c.[DATA_TYPE]= 'nvarchar' and c.[CHARACTER_MAXIMUM_LENGTH] = -1 then '['+ col.[name] + '] ['+ c.[DATA_TYPE]  + ']'+'(max)'
			when c.[DATA_TYPE]= 'varchar' and c.[CHARACTER_MAXIMUM_LENGTH] = -1 then '['+ col.[name] + '] ['+ c.[DATA_TYPE]+ ']'  + '(max)'
			when c.[DATA_TYPE]= 'varbinary' and c.[CHARACTER_MAXIMUM_LENGTH] = -1 then'['+ col.[name] + '] ['+  c.[DATA_TYPE]+ ']'  + '(max)'
			else '['+ col.[name] + '] ['+c.[DATA_TYPE]+ ']'  + '(' + Convert(varchar(200),c.[CHARACTER_MAXIMUM_LENGTH]) + ')' end as index_declare_statement,
			'i.['+ col.[name] + '] = d.['+ col.[name] + ']' index_equal_statement,
			'coalesce(i.['+ col.[name] + '] ,d.['+ col.[name] + '])' index_coalesce_statement,
			'hst.['+ col.[name] + '] = d.['+ col.[name] + ']' index_join_hst_d_statement,
			'r.['+ col.[name] + '] = hst.['+ col.[name] + ']' index_join_hst_R_statement,
			'r.['+ col.[name] + '] = i.['+ col.[name] + ']' index_join_inserted_statement,
			'r.['+ col.[name] + '] = d.['+ col.[name] + ']' index_join_deleted_statement
	
from sys.tables tab
    inner join sys.indexes pk
        on tab.object_id = pk.object_id 
        and pk.is_primary_key = 1
    inner join sys.index_columns ic
        on ic.object_id = pk.object_id
        and ic.index_id = pk.index_id
    inner join sys.columns col
        on pk.object_id = col.object_id
        and col.column_id = ic.column_id
	inner join tables_in_scope ts on ts.Scheme = schema_name(tab.schema_id) and ts.Tbl = tab.name
	inner join [INFORMATION_SCHEMA].[COLUMNS] c on c.TABLE_SCHEMA = schema_name(tab.schema_id) and c.TABLE_NAME = tab.name and c.COLUMN_NAME = col.[name]
	)
,
all_columns as (
Select c.TABLE_SCHEMA, c.TABLE_NAME,c.COLUMN_NAME,c.ORDINAL_POSITION, c.IS_NULLABLE, c.DATA_TYPE, case when pk.column_name is not null then 'PK' else 'NPK' end as Column_type
 FROM [INFORMATION_SCHEMA].[COLUMNS] c
  join tables_in_scope ts on ts.Scheme=c.table_schema and ts.Tbl = c.TABLE_NAME
  left join primary_keys pk 
   on pk.schema_name = c.TABLE_SCHEMA and pk.table_name = c.TABLE_NAME and pk.column_name = c.COLUMN_NAME
)
,
non_key_columns as (
Select * from all_columns ac where ac.column_type = 'NPK'
)

,trulyupdaterows_declare as (  
 select schema_name,table_name, 
    'CREATE TRIGGER ['+schema_name+'].[T_'+table_name +'_HST]
   ON  ['+schema_name+'].['+table_name+']
   AFTER INSERT,DELETE,UPDATE
AS 
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	
	--getting operation type
	DECLARE @Operation char(1) = 
    CASE WHEN EXISTS(SELECT * FROM inserted) AND EXISTS(SELECT * FROM deleted) 
        THEN ''U''
    WHEN EXISTS(SELECT * FROM inserted) 
        THEN ''I''
    WHEN EXISTS(SELECT * FROM deleted)
        THEN ''D''
    ELSE 
        NULL --Unknown
    END;
    

	DECLARE @TransactionTimestamp datetime = getdate()'+CHAR(13) as [1_start]
	,'	declare @trulyupdaterows table ('+STRING_AGG(index_declare_statement, ',' +CHAR(13))+');'+CHAR(13) [2_trulyupdaterows_declare]
	,'	INSERT INTO @trulyupdaterows '+CHAR(13) +'	Select '+STRING_AGG(index_coalesce_statement, ',' +CHAR(13)+' ')+ CHAR(13) + '	from inserted i' + CHAR(13) +'	 full outer join deleted d' + CHAR(13)+'	  on ' +STRING_AGG(index_equal_statement, ' and ' +CHAR(13) + '	  ') +CHAR(13)+ '	where ' [3_trulyupdaterows_insert]
 	,CHAR(13)+' --terminate current open intervalls, created before current transaction' +CHAR(13)+CHAR(13)+'	UPDATE hst
	SET hst.validTo = @TransactionTimestamp
	FROM ['+schema_name+'].['+table_name+'_HST] hst
	 inner join deleted d on ' + STRING_AGG(index_join_hst_d_statement, ' and ' +CHAR(13) + '  ') +'
	 inner join @trulyupdaterows r on '+ STRING_AGG(index_join_hst_r_statement, ' and ' +CHAR(13) + '  ') +'
	where hst.validTo is null and hst.validFrom < @TransactionTimestamp' + CHAR(13) [5_Update_sql],
	'	SELECT ' + STRING_AGG('r.' + pk.column_name, ', ' +CHAR(13) + '  ') + ',' [7_Insert_select_index],
	'	 left join inserted i on ' + STRING_AGG(index_join_inserted_statement, ', ') + CHAR(13) [9_Insert_select_index],
	'	 left join deleted  d on ' + STRING_AGG(index_join_deleted_statement, ', ')+ CHAR(13) [10_Insert_select_index]
  from primary_keys pk
  group by schema_name,table_name
 )

 ,truly_updated_comparison as (
   select table_schema, 
   table_name,
   case 
    when nkc.IS_NULLABLE = 'YES' 
     then '	 ('+CHAR(13)+'	  (i.['+ nkc.COLUMN_NAME +'] is null and d.['+ nkc.COLUMN_NAME +'] is not null) or '+CHAR(13)+'	  (i.['+ nkc.COLUMN_NAME +'] is not null and d.['+ nkc.COLUMN_NAME +'] is null) or'+CHAR(13)+'	   i.['+ nkc.COLUMN_NAME +'] <> d.['+nkc.COLUMN_NAME+']'+CHAR(13)+'	 )'
   
    when nkc.IS_NULLABLE = 'NO' 
     then '	  i.['+ nkc.COLUMN_NAME +'] <> d.['+nkc.COLUMN_NAME+']'
   END as comparison
     
   from non_key_columns nkc
   --> evt TODO --> erweiterung, damit "ungewollte" Spalten nich geprüft werden
   )--   Select * from truly_updated_comparison
,truly_updated_comparison_per_table as (
  select table_schema, table_name,STRING_AGG(cast(comparison as nvarchar(max)),' or'+CHAR(13)) trulyupdaterows_insert_2
  from truly_updated_comparison 
  group by table_schema,table_name)
,insert_start as(
 select table_schema, table_name, CHAR(13)+ '	INSERT INTO ['+table_schema+'].['+table_name+'_HST] ('+
   string_agg('['+column_name+']',CHAR(13)+'	 ,') + CHAR(13)+'	 ,[sqlAction]'+CHAR(13)+'	 ,[validFrom])' +CHAR(13) [6_Insert_start] 
   from all_columns group by table_schema,table_name
   )
,insert_select_npk as(
 select table_schema, table_name, CHAR(13)+'	  '+
   string_agg('i.['+column_name+']',CHAR(13)+'	  ,') + CHAR(13)+'	  ,@Operation'+CHAR(13)+'	  ,@TransactionTimestamp' + CHAR(13) + '	from @trulyupdaterows r' + CHAR(13) [8_Insert_select_npk] 
   from non_key_columns group by table_schema,table_name
   )
 Select d.schema_name,d.table_name, d.[1_start]+ d.[2_trulyupdaterows_declare]+d.[3_trulyupdaterows_insert]+ut.trulyupdaterows_insert_2+d.[5_Update_sql]+ist.[6_Insert_start]+ d.[7_Insert_select_index]+inp.[8_Insert_select_npk]+d.[9_Insert_select_index]+d.[10_Insert_select_index]+'
END
GO' trigger_sql,
'ALTER TABLE ['+d.schema_name+'].['+d.table_name+'] ENABLE TRIGGER [T_'+d.table_name+'_HST] GO' enable_sql
 from trulyupdaterows_declare d
 left join truly_updated_comparison_per_table ut on d.schema_name = ut.TABLE_SCHEMA and d.table_name = ut.TABLE_NAME
 left join insert_start ist on d.schema_name = ist.TABLE_SCHEMA and d.table_name = ist.TABLE_NAME
 left join insert_select_npk inp on d.schema_name = inp.TABLE_SCHEMA and d.table_name = inp.TABLE_NAME
