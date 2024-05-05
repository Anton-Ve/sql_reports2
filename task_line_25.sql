COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := 'acct_25';
   l_SHEET_NM   VARCHAR2(32) := 'Д/п не равен 100(1)';    
   l_SHEET_COL  VARCHAR2(32) := '';
   l_SQL_OP     VARCHAR2(7)  := 'CREATE';
   l_ENABLE     number := 1;
   l_EXPORT	    number := 1;
   l_LINE_BUILD number := 1;
   l_DESCR      clob := Q'##';
   l_SQL_HEADER clob := Q'##';
   l_SQL_QUERY  clob := Q'#select * from ${acct_25}#';
   l_SQL_DATA   clob := Q'#
  select * from (
select tmp.cis_division
      , (
          select descr
            from ci_acct_char  ac
               , ci_char_val_l cvl
           where 1 = 1 
             and ac.acct_id = sa.acct_id
             and ac.char_type_cd = 'OTDELEN'
             and ac.effdt = (
                 select max(effdt)
                   from ci_acct_char a1
                  where 1 = 1
                    and a1.acct_id = ac.acct_id
                    and a1.char_type_cd = ac.char_type_cd
             )
             and cvl.char_type_cd = ac.char_type_cd
             and cvl.char_val = ac.char_val
             and cvl.language_cd = 'RUS'
      ) "Отделение"
      , (
          select descr
            from ci_acct_char  ac
               , ci_char_val_l cvl
           where 1 = 1 
             and ac.acct_id = sa.acct_id
             and ac.char_type_cd = 'U4ASTOK'
             and ac.effdt = (
                 select max(effdt)
                   from ci_acct_char a1
                  where 1 = 1
                    and a1.acct_id = ac.acct_id
                    and a1.char_type_cd = ac.char_type_cd
             )
             and cvl.char_type_cd = ac.char_type_cd
             and cvl.char_val = ac.char_val
             and cvl.language_cd = 'RUS'      
      ) "Участок"   
      , (
            select descr
              from ci_sp_type_l spl
             where 1 = 1
               and spl.sp_type_cd = sp.sp_type_cd
               and spl.language_cd = 'RUS' 
      ) "Тип ТУ"
      , sp.sp_id      "Идентификатор точки учета"
      , tmp.sap_dproc "Сумма значений ДПИ"
      , sa.sa_id      "ID РДО"
      , ( 
           select descr
             from ci_sa_type_l stl
            where 1 = 1
              and stl.sa_type_cd = sa.sa_type_cd
              and stl.cis_division = sa.cis_division
              and stl.language_cd = 'RUS' 
      ) "Тип РДО"
      , (
            select rsl.descr
              from ci_sa_rs_hist rsh
                 , ci_rs_l       rsl
             where 1 = 1
               and rsh.sa_id = sa.sa_id
               and rsh.effdt = (
                   select max(effdt)
                     from ci_sa_rs_hist h1
                    where 1 = 1
                      and h1.sa_id = rsh.sa_id 
               )  
               and rsl.rs_cd = rsh.rs_cd
               and rsl.language_cd = 'RUS'
      ) "План расчета"
      , (
             select adhoc_char_val
              from ci_sa_sp_char sc
             where 1 = 1
               and sc.sa_sp_id = sap.sa_sp_id
               and sc.char_type_cd = 'DPROC'
               and sc.effdt = (
                   select max(effdt)
                     from ci_sa_sp_char s1
                    where 1 = 1
                      and s1.sa_sp_id = sc.sa_sp_id
                      and s1.char_type_cd = sc.char_type_cd 
               )
      ) "Дробный процент использования"
     , tmp.mm 
  from ${acct_21} tmp
     , ci_sa         sa
     , ci_sa_sp      sap
     , ci_sp         sp
 where 1 = 1
   and sap.sp_id = tmp.sp_id
   and sap.stop_dttm is null
   and sp.sp_id = sap.sp_id
   and sa.sa_id = sap.sa_id
   and tmp.mm >= to_date('01.07.2021', 'dd.mm.yyyy')
   and tmp.cnt > 1
   and exists (
       select 1
         from ci_sa_rs_hist h1
        where 1 = 1
         and tmp.cis_division in (select /*+ cardinality 1*/ CAST(Trim(r.VL) as char(5)) cis_division from TABLE( VZ_REPORT_API.split('${PARAMETER_01}')) r) 
          and h1.sa_id = sa.sa_id
          and h1.rs_cd = tmp.rs_hist 
   )
)
where "Сумма значений ДПИ" <> 100
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
