begin 
  VZ_REPORT_QUERY.g_log := true;
  VZ_REPORT_QUERY.SYS_TYPE := 'blob';
  VZ_REPORT_QUERY.TASK_CAT := '';
  VZ_REPORT_QUERY.TASK_BASE_CD := '09.025';
  VZ_REPORT_QUERY.PARAMETER_01 := 'SE-A';
  VZ_REPORT_QUERY.PARAMETER_02 := ''; 
  VZ_REPORT_QUERY.PARAMETER_03 := '';
  VZ_REPORT_QUERY.PARAMETER_04 := '';
  VZ_REPORT_QUERY.PARAMETER_05 := '';
  VZ_REPORT_QUERY.FILTER_FOR_TEST := '';
  --
  if VZ_REPORT_QUERY.generate_data('erofeev' ) then
    null;
  end if;
  dbms_output.put_line( 'ID='||VZ_REPORT_QUERY.report_id_md5() );
end;
