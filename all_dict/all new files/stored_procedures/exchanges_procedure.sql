create or replace PROCEDURE buildExchnages()
LANGUAGE SQL
as $$
	INSERT INTO exchanges(base, exchanges) (
		select symbol, exchanges from public.exchangesdata()
	)
	on conflict (base) do Update set 
	base = EXCLUDED.base,
	exchanges = EXCLUDED.exchanges
$$;

CALL  buildExchnages();