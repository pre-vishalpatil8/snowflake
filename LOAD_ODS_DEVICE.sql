BEGIN

delete from  ODS.PUBLIC.ODS_device wm
using(select distinct plant_id,device_id,DATATIMESTAMP  from rawdata.public.RL_device_data) as rl
where rl.plant_id = wm.plant_id and wm.DATATIMESTAMP = rl.DATATIMESTAMP and rl.device_id = wm.device_id
and wm.baikal_validation is null and wm.error_flag is null;

delete from  ODS.PUBLIC.ODS_device wm
using(select distinct plant_id,device_id,DATATIMESTAMP  from rawdata.public.RL_device_data) as rl
where rl.plant_id = wm.plant_id and wm.DATATIMESTAMP = rl.DATATIMESTAMP and rl.device_id = wm.device_id
and wm.baikal_validation ='Y' and wm.error_flag is null;


create or replace temporary table tmp_ODS_device as
select PLANT_ID, device_id,DATATIMESTAMP,to_date(DATATIMESTAMP) date, DATE_PART(year,DATATIMESTAMP) year, 
        DATE_PART(year,DATATIMESTAMP) fin_year,DATE_PART(week,DATATIMESTAMP) week, DATE_PART(month,DATATIMESTAMP) month,DATE_PART(day,DATATIMESTAMP) day,DATE_PART(hour,DATATIMESTAMP) hour, DATE_PART(minute,DATATIMESTAMP) minute, 
INVERTER_ACTIVE_POWER, Inverter_cabinet_temp, INVERTER_DAILY_ENERGY, INVERTER_PV_POWER, Inverter_Efficiency, Inverter_AC_Frequency, Inverter_Global_Energy, Inverter_Temperature, Inverter_PV_Current, Inverter_PV_Voltage, Inverter_Reactive_Power, Inverter_Specific_Power, Inverter_Total_Energy, Inverter_Specific_Energy, Inverter_AC_Current, Inverter_Power_Factor, Inverter_AC_Voltage, Inverter_PR, ACCAPACITY, DCCAPACITY, INJESTION_FREQUENCY, LAST_MODIFIED_TIME
from rawdata.public.RL_device_data;


update tmp_ODS_device 
set fin_year = fin_year - 1
where month in (1,2,3)
and plant_id in (select  plant_id from FIN_YEAR_APRIL_PLANTS);




insert into ODS.PUBLIC.ODS_device
(PLANT_ID,device_id,DATATIMESTAMP,DATEVALUE,YEAR_NO,fin_year, WEEK_NO,MONTH_NO,DAY_NO,HOUR_NO,MINUTE_NO,INVERTER_ACTIVE_POWER, Inverter_cabinet_temp, INVERTER_DAILY_ENERGY, INVERTER_PV_POWER, Inverter_Efficiency, Inverter_AC_Frequency, Inverter_Global_Energy, Inverter_Temperature, Inverter_PV_Current, Inverter_PV_Voltage, Inverter_Reactive_Power, Inverter_Specific_Power, Inverter_Total_Energy, Inverter_Specific_Energy, Inverter_AC_Current, Inverter_Power_Factor, Inverter_AC_Voltage, Inverter_PR, ACCAPACITY,DCCAPACITY, INJESTION_FREQUENCY, LAST_MODIFIED_TIME, DATA_INSERTION_DATE)
 select   PLANT_ID,device_id,DATATIMESTAMP, date  , year  ,    fin_year, WEEK  ,
   MONTH  , DAY  ,HOUR  ,MINUTE  ,INVERTER_ACTIVE_POWER, Inverter_cabinet_temp, INVERTER_DAILY_ENERGY, INVERTER_PV_POWER, Inverter_Efficiency, Inverter_AC_Frequency, Inverter_Global_Energy, Inverter_Temperature, Inverter_PV_Current, Inverter_PV_Voltage, Inverter_Reactive_Power, Inverter_Specific_Power, Inverter_Total_Energy, Inverter_Specific_Energy, Inverter_AC_Current, Inverter_Power_Factor, Inverter_AC_Voltage, Inverter_PR, ACCAPACITY, DCCAPACITY, INJESTION_FREQUENCY, LAST_MODIFIED_TIME, getdate() as DATA_INSERTION_DATE
  from  tmp_ODS_device;

--Deleting duplicates------------
DELETE FROM ODS.PUBLIC.ODS_device
WHERE (PLANT_ID,   DEVICE_ID,    DATATIMESTAMP,    LAST_MODIFIED_TIME) IN (
  SELECT PLANT_ID,    DEVICE_ID,    DATATIMESTAMP,    LAST_MODIFIED_TIME
  FROM ( SELECT    PLANT_ID,    DEVICE_ID,    DATATIMESTAMP,    LAST_MODIFIED_TIME,
    ROW_NUMBER() OVER (PARTITION BY PLANT_ID, DEVICE_ID, DATATIMESTAMP ORDER BY LAST_MODIFIED_TIME DESC) AS row_num
  FROM
    ODS.PUBLIC.ODS_device) RankedRows
  WHERE row_num > 1
);
--Deleting duplicates------------


INSERT INTO ODS.PUBLIC.ODS_PROCESS_LOG
(PLANT_ID,device_id, DATEVALUE,YEAR_NO,fin_year,MONTH_NO,DAY_NO,HOUR_NO,DATA_FILE_CLASS,DATA_LOAD_ON )
select  DISTINCT PLANT_ID,device_id, date  , year  ,    fin_year, 
   MONTH  , DAY  ,HOUR  ,'DEVICE', GETDATE()
  from  tmp_ODS_device;

RETURN 'Success';
-----------------Enter the Procedure Name explicitly in the values------------------------------
EXCEPTION                                                              
    WHEN statement_error THEN
    insert into ODS.PUBLIC.ODS_JOB_execution_failure_log (SP_NAME,RUN_DATE,ERROR_MESSAGE) 
    values ('ODS.PUBLIC.LOAD_ODS_DEVICE',getdate(),:SQLERRM);
 RETURN :SQLERRM;
 
END