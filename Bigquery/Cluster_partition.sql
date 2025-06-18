CREATE OR REPLACE TABLE `api-project-901373404215.digital_assets.Community_Member_Info_test1`
PARTITION BY DATE(registration_date)
CLUSTER BY wallet_addr, member_id
AS
SELECT * FROM `api-project-901373404215.digital_assets.Community_Member_Info_test`


-- test the rows
SELECT COUNT(*) FROM `api-project-901373404215.digital_assets.Community_Member_Info_test1`

-- rename the old table to drop it 
ALTER TABLE `api-project-901373404215.digital_assets.Community_Member_Info_test`
RENAME TO `Community_Member_Info_test4`;

-- rename the new table to target table 
ALTER TABLE `api-project-901373404215.digital_assets.Community_Member_Info_test1`
RENAME TO `Community_Member_Info_test`;

-- drop the old table
DROP TABLE `api-project-901373404215.digital_assets.Community_Member_Info_test4`






-- dev table 
CREATE OR REPLACE TABLE `api-project-901373404215.digital_assets.Community_Member_Info_dev1`
PARTITION BY DATE(registration_date)
CLUSTER BY wallet_addr, member_id
AS
SELECT * FROM `api-project-901373404215.digital_assets.Community_Member_Info_dev`


SELECT COUNT(*) FROM `api-project-901373404215.digital_assets.Community_Member_Info_dev1`


ALTER TABLE `api-project-901373404215.digital_assets.Community_Member_Info_dev`
RENAME TO `Community_Member_Info_test4`;

ALTER TABLE `api-project-901373404215.digital_assets.Community_Member_Info_dev1`
RENAME TO `Community_Member_Info_dev`;


DROP TABLE `api-project-901373404215.digital_assets.Community_Member_Info_test4`


-- prd table 
CREATE OR REPLACE TABLE `api-project-901373404215.digital_assets.Community_Member_Info1`
PARTITION BY DATE(registration_date)
CLUSTER BY wallet_addr, member_id
AS
SELECT * FROM `api-project-901373404215.digital_assets.Community_Member_Info`


SELECT COUNT(*) FROM `api-project-901373404215.digital_assets.Community_Member_Info1`
SELECT COUNT(*) FROM `api-project-901373404215.digital_assets.Community_Member_Info`


ALTER TABLE `api-project-901373404215.digital_assets.Community_Member_Info`
RENAME TO `Community_Member_Info_test4`;

ALTER TABLE `api-project-901373404215.digital_assets.Community_Member_Info1`
RENAME TO `Community_Member_Info`;


DROP TABLE `api-project-901373404215.digital_assets.Community_Member_Info_test4`
