-- MERGE INTO
--   api-project-901373404215.digital_assets.Community_Member_Info_test T
-- USING
--   api-project-901373404215.digital_assets.Community_Member_Info_dev S
-- ON
--   T.member_id = S.member_id
--   AND T.email_addr = S.email_addr
--   AND T.display_name = S.display_name
--   AND T.registration_date = S.registration_date
--   AND T.free_trial_end_date = S.free_trial_end_date
--   WHEN NOT MATCHED THEN INSERT (member_id, email_addr, display_name, registration_date, free_trial_end_date, row_last_updated) VALUES (S.member_id, S.email_addr, S.display_name, S.registration_date, S.free_trial_end_date, S.row_last_updated)
--   WHEN MATCHED
--   THEN
-- UPDATE
-- SET
--   T.email_addr = S.email_addr,
--   T.display_name = S.display_name,
--   T.registration_date = S.registration_date,
--   T.free_trial_end_date = S.free_trial_end_date,
--   T.row_last_updated = S.row_last_updated;

MERGE INTO `api-project-901373404215.digital_assets.Community_Member_Info_test` AS target
USING (
  SELECT
    member_id,
    email_addr,
    display_name,
    registration_date,
    free_trial_end_date,
    row_last_updated
  FROM
    `api-project-901373404215.digital_assets.Community_Member_Info_dev`
) AS source
ON target.member_id = source.member_id
WHEN MATCHED THEN
  UPDATE SET
    email_addr = source.email_addr,
    display_name = source.display_name,
    registration_date = source.registration_date,
    free_trial_end_date = source.free_trial_end_date,
    row_last_updated = source.row_last_updated
WHEN NOT MATCHED THEN
  INSERT (member_id, email_addr, display_name, registration_date, free_trial_end_date, row_last_updated)
  VALUES (
    source.member_id,
    source.email_addr,
    source.display_name,
    source.registration_date,
    source.free_trial_end_date,
    source.row_last_updated
  );

-- delete duplicate data 
DELETE FROM `api-project-901373404215.digital_assets.Community_Member_Info_dev` 
WHERE (member_id) IN (
  SELECT member_id
  FROM (
    SELECT
      member_id,
      ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY member_id) AS row_number
    FROM
      api-project-901373404215.digital_assets.Community_Member_Info_dev
  ) AS t
  WHERE t.row_number > 1
);


-- working query for all table
MERGE INTO
  `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated` TARGET
USING
  (
  SELECT
    name,
    forbes_id,
    Occurance_Time
  FROM (
    SELECT
      name,
      forbes_id,
      Occurance_Time,
      ROW_NUMBER() OVER(PARTITION BY name ORDER BY Occurance_Time DESC) AS rn
    FROM
      `api-project-901373404215.digital_assets.Digital_Asset_MarketData_dev_updated` )
  WHERE
    rn = 1 ) SOURCE
ON
  target.name = source.name
WHEN MATCHED THEN
  UPDATE
  SET
    target.forbes_id = source.forbes_id