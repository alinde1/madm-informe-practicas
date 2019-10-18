--
-- Execute HiveQL scripts
--

-- Create table
!echo Creating table...;
source ${hivevar:current_path}/01-create_table.ql;

-- Create temporal table
!echo Creating temporal table...;
source ${hivevar:current_path}/02-create_tmp_table.ql;

-- Insert data
!echo Inserting data...;
source ${hivevar:current_path}/03-insert_data.ql;

-- Drop tempral table
!echo Droping temporal table...;
source ${hivevar:current_path}/04-drop_tmp_table.ql;
