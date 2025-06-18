SELECT 
    REGEXP_EXTRACT(GA_pagePath, r'https://www\.forbes\.com/digital-assets/assets/([^/]+)') AS asset_name,
    COUNT(GA_pagePath) AS page_count
FROM 
    `api-project-901373404215.BusinessIntelligence.GA4_DataMart_base`
WHERE 
    GA_pagePath LIKE 'https://www.forbes.com/digital-assets/assets/%'
GROUP BY 
    asset_name
ORDER BY 
    page_count DESC
-- LIMIT 10;