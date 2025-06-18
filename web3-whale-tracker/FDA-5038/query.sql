-- delete duplicate data 
DELETE
FROM
  `api-project-901373404215.digital_assets.Digital_Asset_Transactions_data_test`
WHERE
  transaction_hash IN (
  SELECT
    transaction_hash
  FROM (
    SELECT
      transaction_hash,
      ROW_NUMBER() OVER (PARTITION BY transaction_hash ORDER BY block_timestamp DESC) AS rn
    FROM
      `api-project-901373404215.digital_assets.Digital_Asset_Transactions_data_test` ) as RankedTransactions
  WHERE
    rn > 1 );