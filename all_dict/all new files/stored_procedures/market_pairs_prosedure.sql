create or replace PROCEDURE buildMaketPairs()
LANGUAGE SQL
as $$
	INSERT INTO market_pairs(base, market_pairs) (
		select symbol, marketPairs from public.MarketPairsData()
	)
	on conflict (base) do Update set 
	base = EXCLUDED.base,
	market_pairs = EXCLUDED.market_pairs
$$;

CALL  buildMaketPairs();