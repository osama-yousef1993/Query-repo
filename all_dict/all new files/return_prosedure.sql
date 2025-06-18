DROP PROCEDURE test_proc_3;
CREATE OR replace PROCEDURE test_proc_3()
AS
$$
DECLARE
    symbol_test text;
	name_test text;
	slug_test text; 
	logo_test text; 
	display_symbol_test text;
	price_24h_test float;
	percentage_24h_test float; 
	change_value_24h_test float;
BEGIN
	RETURN query
    SELECT  
		symbol,
		name, 
		slug,
		logo,
		display_symbol,
		price_24h,
		percentage_24h,
		change_value_24h
	INTO symbol_test, name_test, slug_test, logo_test, display_symbol_test, price_24h_test, percentage_24h_test, change_value_24h_test
	FROM  (
		SELECT  
				distinct 
				symbol,
				row_number() OVER(PARTITION BY symbol ORDER BY last_updated desc) AS row_num,
				name, 
				slug,
				logo,
				display_symbol,
				price_24h,
				percentage_24h,
				change_value_24h,
				market_cap
			FROM    fundamentals
			where market_cap is not null
			And last_updated >= cast(now() - interval '1 HOUR' as timestamp)
			order by market_cap desc
		) as fund
	WHERE  row_num = 1
	limit 200;
END;
$$ LANGUAGE plpgsql;


DO
 $$
 DECLARE
 	symbol_test text;
 	name_test text;
 	slug_test text; 
	logo_test text; 
 	display_symbol_test text;
 	price_24h_test float;
	percentage_24h_test float; 
 	change_value_24h_test float;
 BEGIN
 CALL test_proc_3(symbol_test, name_test, slug_test, logo_test, display_symbol_test, price_24h_test, percentage_24h_test, change_value_24h_test);
 END
$$;

CALL test_proc_3();

