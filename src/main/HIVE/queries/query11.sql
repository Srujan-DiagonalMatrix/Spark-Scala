-- This module is to generate status for each process which is currently in execution or completed

add jar /usr/hdp/current/hive-client/lib/hive-contrib.jar;

USE ${MBNA_CUS_MI_HIVE_DB_NAME};

INSERT INTO TABLE ${MBNA_CUS_MI_HIVE_DB_NAME}.batch_event_process PARTITION(event_name)
SELECT 
  '${job_name}', 
  'NA', 
  'NA', 
  current_timestamp(), 
  NULL, 
  '${job_State}',
  'mbnausr',
  '${event_name}'
FROM
  (select 1) a;