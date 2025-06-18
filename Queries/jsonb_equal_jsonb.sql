SELECT id, slug, tickers, questions, last_updated from nftdatalatest
		where 
		tickers is null
		or 
		tickers::jsonb = '[]'::jsonb

		order by last_updated desc