COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := 'acct_19';
   l_SHEET_NM   VARCHAR2(32) := '';    
   l_SHEET_COL  VARCHAR2(32) := '';
   l_SQL_OP     VARCHAR2(7)  := 'PLSQL';
   l_ENABLE     number := 1;
   l_EXPORT	    number := 0;
   l_LINE_BUILD number := 1;
   l_DESCR      clob := Q'##';
   l_SQL_HEADER clob := Q'##';
   l_SQL_QUERY  clob := Q'##';
   l_SQL_DATA   clob := Q'#DECLARE
  cursor c_sp is
    select distinct 
           sap.sp_id
      from ci_sa_sp      sap
         , ci_sa_sp_char ssc
     where 1 = 1
       and ssc.sa_sp_id = sap.sa_sp_id
       and ssc.char_type_cd = 'DPROC';

  cursor cur_sap ( a_sp_id   ci_sp.sp_id%TYPE ) is
    select *
      from
      (
            select distinct start_dttm as dt, 'O' as action from ci_sa_sp sap where sap.sp_id = a_sp_id
            union
            select distinct trunc(stop_dttm + 1/86400) + 1  as dt, 'C' as action from ci_sa_sp sap where sap.sp_id = a_sp_id 
      ) tt
     where 1  = 1
     order by dt;
  
  cursor c_effdt ( a_sp_id ci_sp.sp_id%TYPE, a_beg_dt date, a_end_dt date ) is
    select distinct
           effdt
      from ci_sa_sp_char ssc
     where 1 = 1
       and ssc.sa_sp_id in (
           select sa_sp_id from ci_sa_sp s where s.sp_id = a_sp_id 
       )
       and ssc.char_type_cd = 'DPROC'
       and ssc.effdt > a_beg_dt          
       and ssc.effdt < a_end_dt;
       
  l_prev_dt   date;
  l_curr_dt   date;     
BEGIN
  delete from ${acct_18};  
  for sp in c_sp loop
  l_prev_dt := to_date('31.12.5999', 'dd.mm.yyyy');
  l_curr_dt := to_date('31.12.5999', 'dd.mm.yyyy');
    
  for rc in cur_sap (sp.sp_id) loop
    l_curr_dt := nvl(rc.dt, to_date('31.12.5999', 'dd.mm.yyyy'));
    insert into ${acct_18} (
        sp_id
       ,dt
       ,src 
    ) values (
        sp.sp_id
       ,l_curr_dt
       ,rc.action 
    );
    if l_prev_dt <> to_date('31.12.5999', 'dd.mm.yyyy') then
      for dt in c_effdt ( sp.sp_id, l_prev_dt, l_curr_dt ) loop
        insert into ${acct_18} (
            sp_id
           ,dt
           ,src 
        ) values (
            sp.sp_id
           ,dt.effdt
           ,'D' 
        );
      end loop;   
    end if;    
    l_prev_dt := l_curr_dt;      
  end loop;    
  end loop;
  commit;
END;
   #';
   l_index number;
begin
    SELECT t.idx into l_index from VZ_TASK_BASE t where t.TASK_CD = l_task_cd and t.CAT_CD = l_cat_cd;

	merge into VZ_TASK_BASE_LINE tgt
	using (
	   SELECT l_index      TASK_IDX
            , l_seq 	   SEQ
            , l_ENTITY_NM  ENTITY_NM
            , l_SHEET_NM   SHEET_NM
            , l_SHEET_COL  SHEET_COL
            , l_SQL_OP 	   SQL_OP
            , l_ENABLE 	   ENABLE
            , l_EXPORT 	   EXPORT
            , l_LINE_BUILD LINE_BUILD
            , l_DESCR 	   DESCR
            , l_SQL_HEADER SQL_HEADER
            , l_SQL_QUERY  SQL_QUERY
            , l_SQL_DATA   SQL_DATA
        FROM dual
	) src
	on (tgt.TASK_IDX = src.TASK_IDX and tgt.SEQ = src.SEQ)
	WHEN MATCHED THEN
		UPDATE set tgt.ENTITY_NM       = src.ENTITY_NM
				 , tgt.SHEET_NM        = src.SHEET_NM
                 , tgt.SHEET_COL       = src.SHEET_COL
				 , tgt.SQL_OP          = src.SQL_OP
				 , tgt.ENABLE          = src.ENABLE
				 , tgt.EXPORT          = src.EXPORT
				 , tgt.LINE_BUILD      = src.LINE_BUILD
				 , tgt.DESCR           = src.DESCR
                 , tgt.SQL_HEADER      = src.SQL_HEADER
				 , tgt.SQL_QUERY       = src.SQL_QUERY
				 , tgt.SQL_DATA        = src.SQL_DATA
        WHERE tgt.LINE_BUILD < src.LINE_BUILD
	WHEN NOT MATCHED THEN
		 INSERT( tgt.TASK_IDX, tgt.SEQ, tgt.ENTITY_NM, tgt.SHEET_NM, tgt.SHEET_COL, tgt.SQL_OP, tgt.ENABLE, tgt.EXPORT, tgt.LINE_BUILD, tgt.DESCR, tgt.SQL_HEADER, tgt.SQL_QUERY, tgt.SQL_DATA)  
		 VALUES( src.TASK_IDX, src.SEQ, src.ENTITY_NM, src.SHEET_NM, src.SHEET_COL, src.SQL_OP, src.ENABLE, src.EXPORT, src.LINE_BUILD, src.DESCR, src.SQL_HEADER, src.SQL_QUERY, src.SQL_DATA)  
	;
	commit;
end;
/
