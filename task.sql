PROMPT VZ_TASK_BASE.merge.&TASK_CAT..&TASK_CD. - start
declare
   l_index number;
   l_build number := &TASK_BUILD.;
   l_task_cd VARCHAR2(16)  := '&TASK_CD.';
   l_cat_cd VARCHAR2(16)   := '&TASK_CAT.';
   l_cat_nm VARCHAR2(4000) := 'ќшибки продуктива';
   l_param_set_id number   := 1;
   l_flag  varchar2(1)     := 'A';
   l_cat_descr clob := Q'#ќшибки продуктива.#';
--
   l_current_build number;
begin
    begin 
      SELECT t.idx into l_index from VZ_TASK_BASE t where t.TASK_CD = l_task_cd and t.CAT_CD = l_cat_cd;
    exception WHEN NO_DATA_FOUND THEN
      l_index := CM_TASK_IDS.Nextval;
    end;
    
    select max(b.TASK_BUILD) into l_current_build from VZ_TASK_BASE b where b.IDX = l_index;
    if nvl( l_current_build, -1) < l_build then
      delete from VZ_TASK_BASE_LINE bl where bl.TASK_IDX = l_index;
      commit;
    end if;
    
	merge into VZ_TASK_BASE tgt
	using (
	   SELECT l_index        IDX
            , l_task_cd      TASK_CD
            , l_cat_cd       CAT_CD
            , l_cat_nm       NM
            , l_cat_descr    DESCR
            , l_param_set_id PARAM_SET_IDX
            , l_flag         FLG
            , l_build        TASK_BUILD
        FROM dual
	) src
	on (tgt.IDX = src.IDX)
	WHEN MATCHED THEN
		UPDATE set tgt.TASK_CD        = src.TASK_CD
				 , tgt.CAT_CD         = src.CAT_CD
				 , tgt.NM             = src.NM
				 , tgt.DESCR          = src.DESCR
				 , tgt.PARAM_SET_IDX  = src.PARAM_SET_IDX
				 , tgt.FLG            = src.FLG
				 , tgt.TASK_BUILD     = src.TASK_BUILD
        WHERE tgt.TASK_BUILD < src.TASK_BUILD
	WHEN NOT MATCHED THEN
		 INSERT( tgt.IDX, tgt.TASK_CD, tgt.CAT_CD, tgt.NM, tgt.DESCR, tgt.PARAM_SET_IDX, tgt.FLG, tgt.TASK_BUILD, tgt.LAST_DT )  
		 VALUES( src.IDX, src.TASK_CD, src.CAT_CD, src.NM, src.DESCR, src.PARAM_SET_IDX, src.FLG, src.TASK_BUILD, sysdate )
	;
	commit;
end;
/
