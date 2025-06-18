with high_low_price_oneDay AS (
  select
    id,
    beprices_1d[OFFSET(0)].floorpriceusd AS lowest_floor_price_24h_usd,
    beprices_1d[OFFSET(0)].floorpricenative AS lowest_floor_price_24h_native,
    beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd AS highest_floor_price_24h_usd,
    beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative AS highest_floor_price_24h_native,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpriceusd - beprices_1d[OFFSET(0)].floorpriceusd) / NULLIF(beprices_1d[OFFSET(0)].floorpriceusd, 0)) * 100 AS floor_price_24h_percentage_change_usd,
    ((beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - 1)].floorpricenative - beprices_1d[OFFSET(0)].floorpricenative) / NULLIF(beprices_1d[OFFSET(0)].floorpricenative, 0)) * 100 AS floor_price_24h_percentage_change_native,
    beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - cast((ARRAY_LENGTH(beprices_1d)/2) as int64))].floorpriceusd AS highest_floor_price_24h_usd_yesterday,
    beprices_1d[OFFSET(ARRAY_LENGTH(beprices_1d) - cast((ARRAY_LENGTH(beprices_1d)/2) as int64))].floorpricenative AS highest_floor_price_24h_native_yesterday,
  from(
    SELECT
      id,
      ARRAY_AGG(STRUCT( 'Time',
          time,
          'floorpriceusd',
          floorpriceusd,
          'floorpricenative',
          floorpricenative )
      ORDER BY
        time) AS beprices_1d,
    FROM (
      SELECT
        id,
        Occurance_Time AS time,
        CAST(floorprice_usd AS FLOAT64) floorpriceusd,
        CAST(floorprice_native AS FLOAT64) floorpricenative,
        ROW_NUMBER() OVER (PARTITION BY ID, CAST(FLOOR(UNIX_SECONDS(Occurance_Time)/ 900 )AS INT64 )
        ORDER BY
          Occurance_Time ) AS row_num
      FROM
        api-project-901373404215.digital_assets.Digital_Assets_NFT_MarketData_dev c
      WHERE
        Occurance_Time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 day)
      ORDER BY
        Occurance_Time ) AS test
    WHERE
      row_num = 1
    GROUP BY
      id )

  )
select 
  id,
  lowest_floor_price_24h_usd,
  highest_floor_price_24h_usd,
  floor_price_24h_percentage_change_usd,
  lowest_floor_price_24h_native,
  highest_floor_price_24h_native,
  floor_price_24h_percentage_change_native,
  highest_floor_price_24h_usd_yesterday,
  highest_floor_price_24h_native_yesterday,
  ((highest_floor_price_24h_usd - highest_floor_price_24h_usd_yesterday) / NULLIF(highest_floor_price_24h_usd_yesterday, 0)) * 100 AS highest_floor_price_24h_percentage_change_usd,
  ((highest_floor_price_24h_native - highest_floor_price_24h_native_yesterday) / NULLIF(highest_floor_price_24h_native_yesterday, 0)) * 100 AS highest_floor_price_24h_percentage_change_native,
  
from 
high_low_price_oneDay

where high_low_price_oneDay.id = 'bored-ape-yacht-club'