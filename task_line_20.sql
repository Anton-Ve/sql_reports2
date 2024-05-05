COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := 'acct_20';
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
  cursor c_dt is
     select * from ${acct_18} t order by t.sp_id, t.dt;
  cursor c_row_o ( a_sp_id  ci_sp.sp_id%TYPE, a_effdt date ) is
    select sap.sa_sp_id
         , sap.sp_id
     , sap.sa_id
     , sap.start_dttm
     , case
          when nvl(sap.stop_dttm, to_date('01.01.2100', 'dd.mm.yyyy')) >= a_effdt then null
          else sap.stop_dttm  
       end stop_dttm    
     , ssc.effdt
     , to_number(replace(replace(nvl(regexp_replace(ssc.adhoc_char_val, '[^[[:digit:],.]]*'), '0'), ',', '.'), ' ', '')) as val
  from ci_sa_sp      sap
     , ci_sa_sp_char ssc
 where 1 = 1
   and sap.sp_id = a_sp_id
   and sap.start_dttm <= a_effdt
   and nvl(sap.stop_dttm, to_date('31.12.5999', 'dd.mm.yyyy')) > a_effdt
   and ssc.sa_sp_id = sap.sa_sp_id   
   and ssc.char_type_cd = 'DPROC'
   and ssc.effdt = (
       select max(effdt)
         from ci_sa_sp_char s1
        where 1 = 1
          and s1.sa_sp_id = ssc.sa_sp_id
          and s1.char_type_cd = ssc.char_type_cd
          and s1.effdt <= a_effdt 
   )
   and exists (
       select 1
         from ci_sa s20
        where 1 = 1
          and s20.sa_status_flg <> '70'
          and s20.sa_id = sap.sa_id 
   );
   
  cursor c_row_c ( a_sp_id  ci_sp.sp_id%TYPE, a_effdt date ) is
select sap.sa_sp_id
     , sap.sp_id
     , sap.sa_id
     , sap.start_dttm
     , case
          when nvl(sap.stop_dttm, to_date('01.01.2100', 'dd.mm.yyyy')) >= a_effdt then null
          else sap.stop_dttm  
       end stop_dttm    
     , ssc.effdt
     , to_number(replace(replace(nvl(regexp_replace(ssc.adhoc_char_val, '[^[[:digit:],.]]*'), '0'), ',', '.'), ' ', '')) as val
  from ci_sa_sp      sap
     , ci_sa_sp_char ssc
 where 1 = 1
   and sap.sp_id = a_sp_id
   and sap.start_dttm <= a_effdt
   and nvl(sap.stop_dttm, to_date('31.12.5999', 'dd.mm.yyyy')) > a_effdt 
   and ssc.sa_sp_id = sap.sa_sp_id   
   and ssc.char_type_cd = 'DPROC'
   and ssc.effdt = (
       select max(effdt)
         from ci_sa_sp_char s1
        where 1 = 1
          and s1.sa_sp_id = ssc.sa_sp_id
          and s1.char_type_cd = ssc.char_type_cd
          and s1.effdt <= a_effdt --nvl(sap.stop_dttm, to_date('31.12.5999', 'dd.mm.yyyy')) 
   )
   and exists (
       select 1
         from ci_sa s20
        where 1 = 1
          and s20.sa_status_flg <> '70'
          and s20.sa_id = sap.sa_id 
   );
      
   rn  number;
BEGIN
  rn := 0;
  delete from ${acct_17};
  
  for dt in c_dt loop
  begin  
    rn := rn + 1;
    if ( dt.src = 'O' or dt.src = 'D' ) then
      
      for rc in c_row_o ( dt.sp_id, dt.dt ) loop
        insert into ${acct_17} (
            sp_id
           ,sa_sp_id
           ,sa_id
           ,rs_cd
           ,start_dttm
           ,stop_dttm
           ,beg_dt
           ,effdt
           ,val
           ,period_id 
        ) values (
            rc.sp_id
          , rc.sa_sp_id
          , rc.sa_id
          , nvl((
                 select rs_cd
                   from ci_sa_rs_hist h
                  where 1 = 1
                    and h.sa_id = rc.sa_id
                    and h.effdt = (
                        select max(effdt)
                          from ci_sa_rs_hist h1
                         where 1 = 1
                           and h1.sa_id = h.sa_id
                           and h1.effdt <= dt.dt 
                    ) 
          ), (
                 select rs_cd
                   from ci_sa_rs_hist h
                  where 1 = 1
                    and h.sa_id = rc.sa_id
                    and h.effdt = (
                        select max(effdt)
                          from ci_sa_rs_hist h1
                         where 1 = 1
                           and h1.sa_id = h.sa_id
                    )           
          )) 
          , rc.start_dttm
          , rc.stop_dttm
          , dt.dt
          , rc.effdt
          , rc.val
          , rn  
        );
      end loop;  
        
    end if;  

    if (dt.src = 'C') then
      
      for rc in c_row_c ( dt.sp_id, dt.dt ) loop
        insert into ${acct_17} (
            sp_id
           ,sa_sp_id
           ,sa_id
           ,rs_cd
           ,start_dttm
           ,stop_dttm
           ,beg_dt
           ,effdt
           ,val
           ,period_id 
        ) values (
            rc.sp_id
          , rc.sa_sp_id
          , rc.sa_id
          , nvl((
                 select rs_cd
                   from ci_sa_rs_hist h
                  where 1 = 1
                    and h.sa_id = rc.sa_id
                    and h.effdt = (
                        select max(effdt)
                          from ci_sa_rs_hist h1
                         where 1 = 1
                           and h1.sa_id = h.sa_id
                           and h1.effdt <= dt.dt 
                    ) 
          ), (
                 select rs_cd
                   from ci_sa_rs_hist h
                  where 1 = 1
                    and h.sa_id = rc.sa_id
                    and h.effdt = (
                        select max(effdt)
                          from ci_sa_rs_hist h1
                         where 1 = 1
                           and h1.sa_id = h.sa_id
                    )           
          ))           
          , rc.start_dttm
          , rc.stop_dttm
          , dt.dt
          , rc.effdt
          , rc.val
          , rn  
        );
      end loop;  
        
    end if;  
  exception
    when OTHERS then
      DBMS_OUTPUT.put_line(sqlerrm);  
  end;  
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
