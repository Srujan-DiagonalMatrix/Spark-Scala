-- This module is to generate batch_id and status for each process which is currently in execution

add jar /usr/hdp/current/hive-client/lib/hive-contrib.jar;

USE ${MBNA_CUS_MIG_HIVE_DB_NAME};

--Insert batchid, batchName, jobstatus and eventName into batchevent table from a dummy table <select 1>
INSERT OVERWRITE TABLE ${MBNA_CUS_MIG_HIVE_DB_NAME}.batch_event PARTITION(event)
SELECT ${batch_id}, '${batch_name}', '${job_state}', '${event_name}' from (select 1) a;
