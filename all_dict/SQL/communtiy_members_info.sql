CREATE OR REPLACE FUNCTION getCommunityMemberInfo()
RETURNS Table (
	email_addr integer,
	member_id integer,
	registration_date TIMESTAMPTZ,
	display_name TEXT,
	grant_expiration TIMESTAMPTZ
) AS $func$

with member_data as (
	SELECT 
		wallet_addr, 
		Case when email_addr != '' then 1
		else 0 
		end email_addr,
		member_id,
		registration_date,
		display_name
	FROM community_member_info
	where wallet_addr != ''
	order by member_id asc
),
member_grants as (
	SELECT 
		wallet_addr,
		grant_id,
		grant_expiration
	FROM  member_grants
	where grant_id = 'fin_cry'
	and wallet_addr != ''
	order by grant_id asc
)

select	
	m.email_addr,
	m.member_id,
	m.registration_date,
	m.display_name,
	g.grant_expiration
from 
member_data m
Left Join 
	member_grants g 
ON 
	m.wallet_addr = g.wallet_addr
order by m.member_id asc
$func$ LANGUAGE sql;



-- 
-- 
-- 
-- 
-- 
-- 
-- 
-- 
-- 
-- 
-- 
-- v2
CREATE OR REPLACE FUNCTION getCommunityMemberInfo()
RETURNS Table (
	email_addr integer,
	member_id integer,
	registration_date TIMESTAMPTZ,
	display_name TEXT,
	grant_expiration TIMESTAMPTZ
) AS $func$
SELECT 
	Case when email_addr != '' then 1
	else 0 
	end email_addr,
	member_id,
	registration_date,
	display_name,
	(registration_date + interval '6 months') as grant_expiration 
FROM 
	community_member_info
where 
	wallet_addr != ''
order by 
	member_id asc
$func$ LANGUAGE sql;



clear: both;
background: #ffffff17;
border-radius: 20px;
padding: 20px;
color: white;