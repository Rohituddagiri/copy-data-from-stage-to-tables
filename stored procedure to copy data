CREATE OR REPLACE PROCEDURE DEMO_DATABASE.PUBLIC.SP_LOAD_PARALLEL(TAB_SCHEMA VARCHAR(100), TAB_NAME VARCHAR(100), AWS_SCHEMA VARCHAR(100), FILE_FORMAT_NM VARCHAR(50))
RETURNS VARCHAR
LANGAUGE JAVASCRIPT
AS
$$
TRY
{
var LOAD_STMT = null;
if (TAB_SCHEMA!= null && TAB_NAME!= null && AWS_SCHEMA!= null && FILE_FORMAT_NM!= null)
  {
   var TABLE_SCHEMA = TAB_SCHEMA;
   var TABLE_NAME = TAB_NAME;
   var AWS_SCHEMA = AWS_SCHEMA;
   var FILE_FORMAT_NAME = FILE_FORMAT_NM;
   rs=snowflake.execute({sqlText: `select current_timestamp(0)`});
   while(rs.next())
   {
    var start_timestamp = rs.getColumnValue(1);
   }
   var load_stmt = snowflake.execute({sqlText:` COPY INTO DEMO_DATABASE.`+TABLE_SCHEMA+`.`+TABLE_NAME+` FROM @INTERNAL_STAGE_NAME/`+AWS_SCHEMA+`/`+TABLE_NAME+`/ FILE_FORMAT = (FORMAT_NAME = `+FILE_FORMAT_NAME+`);`});
   snowflake.execute({sqlText:`INSERT INTO TABLES_LOAD_SUCCESS_LIST(`+TABLE_SCHEMA+`,`+TABLE_NAME+`,`+start_timestamp+`,current_timestamp(0));`});
   snowflake.execute({sqlText:`update TABLES_LOAD_SUCCESS_LIST set total_time=TIME_FROM_PARTS(0,0,TIMESTAMPDIFF(SECOND,START_TIMESTAMP,END_TIMESTAMP)) WHERE TABLE_SCHEMA
   ='`+TABLE_SCHEMA+`' AND TABLE_NAME='`+TABLE_NAME+`';`});
   RETURN 'SUCCESS';
  }
  else{
  return 'PLEASE PASS VALID INPUTS';
  }
}
catch(err)
{
snowflake.execute({sqlText:`INSERT INTO TABLE_LOAD_ERROR_LIST(`+TABLE_SCHEMA+`,`+TABLE_NAME+`,`+start_timestamp+`,`+err.message+`);`});
return 'failed with errorcode:'+err.code+' message:'+err.message;
}
$$;
