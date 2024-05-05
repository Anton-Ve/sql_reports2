COLUMN TASK_SEQ NEW_V TASK_SEQ_CURRENT
select (&TASK_SEQ_CURRENT.+1) TASK_SEQ from dual;

PROMPT VZ_TASK_BASE_LINE.&TASK_CAT..&TASK_CD..&TASK_SEQ_CURRENT..merge - start
declare
   l_seq        number := &TASK_SEQ_CURRENT;
   l_task_cd    VARCHAR2(16) := '&TASK_CD.';
   l_cat_cd     VARCHAR2(16) := '&TASK_CAT.';
   l_ENTITY_NM  VARCHAR2(32) := 'acct_09';
   l_SHEET_NM   VARCHAR2(32) := 'Несовпадение субъекта(ССВ)';                        --Несовпадение Субъекта на ССВ и в ЛС включённую в данную ССВ
   l_SHEET_COL  VARCHAR2(32) := '';
   l_SQL_OP     VARCHAR2(7)  := 'CREATE';
   l_ENABLE     number := 1;
   l_EXPORT	    number := 1;
   l_LINE_BUILD number := 1;
   l_DESCR      clob := Q'#9#';
   l_SQL_HEADER clob := Q'##';
   l_SQL_QUERY  clob := Q'#select * from ${acct_09}#';
   l_SQL_DATA   clob := 
Q'#
with otdel as 
(
select 
 ac.acct_id
,max(ac.srch_char_val) keep (dense_rank last ordre by ac.effdt asc nulls first) as val
,max(l.descr) keep (dense_rank last ordre by ac.effdt asc nulls first) as descr
from ci_acct_char ac
inner join ci_char_val_l l on l.char_type_cd=ac.char_type_cd and trim(l.char_val)=ac.srch_char_val and l.language_cd='RUS'
where ac.char_type_cd='OTDELEN'
group by ac.acct_id
),
U4 as 
(
select 
 ac.acct_id
,max(ac.srch_char_val) keep (dense_rank last ordre by ac.effdt asc nulls first) as val
,max(l.descr) keep (dense_rank last ordre by ac.effdt asc nulls first) as descr
from ci_acct_char ac
inner join ci_char_val_l l on l.char_type_cd=ac.char_type_cd and trim(l.char_val)=ac.srch_char_val and l.language_cd='RUS'
where ac.char_type_cd='U4ASTOK'
group by ac.acct_id
)
select
l3.descr                as "Филиал"
, otdel.descr           as "Отделение"
, U4.descr              as "Участок"
, scd.stm_cnst_id       as "ИД ССВ"
, sc.per_id             as "ИД Субъекта ССВ"
, pn1.entity_name       as "Имя субъекта ССВ"
, a.acct_id             as "ИД ЛС"
, a.CUST_CL_CD      as " Категория абонента"    
, ap.per_id             as "ИД субъекта ЛС"
, pn2.entity_name       as "Имя субъекта ЛС"
from ci_acct a
inner join ci_acct_per ap on ap.acct_id=a.acct_id and ap.main_cust_sw='Y'
inner join ci_stm_cnst_dtl scd on scd.acct_id=a.acct_id and scd.end_dt is null
inner join ci_stm_cnst sc on sc.stm_cnst_id=scd.stm_cnst_id and sc.eff_status='A'
left join ci_per_name pn1 on pn1.per_id=sc.per_id and pn1.prim_name_sw='Y'
left join ci_per_name pn2 on pn2.per_id=ap.per_id and pn2.prim_name_sw='Y'
left join otdel on otdel.acct_id=a.acct_id
left join U4 on U4.acct_id=a.acct_id
left join ci_cis_division_l l3 on l3.cis_division=a.cis_division and l3.language_cd='RUS'
where 1=1
and a.cis_division in(select /*+ cardinality 1*/ CAST(Trim(r.VL) as char(5)) cis_division from TABLE( VZ_REPORT_API.split('${PARAMETER_01}')) r)
and ap.per_id!=sc.per_id
order by l3.descr, otdel.descr, U4.descr, scd.stm_cnst_id
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
