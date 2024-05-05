PROMPT VZ_TASK_BASE_CAT.link.main.&TASK_CD. - start
merge into VZ_TASK_BASE_CAT tgt
using (
   SELECT t.task_cd
        , 0 seqno
        , t.cat_cd
        , t.task_build build
  FROM VZ_TASK_BASE t
 WHERE t.TASK_CD = '&TASK_CD.'
) src
on (tgt.task_cd = src.task_cd and tgt.seqno = src.seqno)
WHEN MATCHED THEN
  UPDATE set tgt.cat_cd = src.cat_cd
           , tgt.build  = src.build
  WHERE tgt.build < src.build
WHEN NOT MATCHED THEN
   INSERT( tgt.task_cd, tgt.seqno, tgt.cat_cd, tgt.build )  
   VALUES( src.task_cd, src.seqno, src.cat_cd, src.build )
/
commit;