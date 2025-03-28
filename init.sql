CREATE DATABASE stock_db;

\c stock_db;

drop table if exists stock_price_history;
drop table if exists stock_company;

begin;
	create table stock_company (
		company_id int generated always as identity,
		company_name varchar(255),
		symbol varchar(10) not null,
		current_price decimal(18,8),
		current_price_date date,
		market_cap decimal(32,2),
		updated_at timestamp default now(),
		primary key(company_id)
	);
	
	create table stock_price_history(
		history_id int generated always as identity,
		company_id int not null,
		price_date date not null,
		price_open decimal(18,8),
		price_high decimal(18,8),
		price_low decimal(18,8),
		price_close decimal(18,8),
		volume decimal(32,2),
		sma30 decimal(18,8),
		sma60 decimal(18,8),
		crossover_signal varchar(50),
		golden_cross_start_price decimal(18,8),
		golden_cross_end_price decimal(18,8),
		golden_cross_end_date date,
		golden_cross_price_change decimal(18,8),
		golden_cross_price_change_percentage decimal(18,8),
		death_cross_start_price decimal(18,8),
		death_cross_end_price decimal(18,8),
		death_cross_end_date date,
		death_cross_price_change decimal(18,8),
		death_cross_price_change_percentage decimal(18,8),
		primary key(history_id),
		constraint fk_stock_price_history_company_id_stock_company_company_id
			foreign key(company_id)
				references stock_company(company_id)
	);
commit;

INSERT INTO stock_company (company_name, symbol, current_price, current_price_date, market_cap, updated_at) VALUES
('Accenture plc', 'ACN', NULL, NULL, NULL, NOW()),
('Adobe Inc.', 'ADBE', NULL, NULL, NULL, NOW()),
('Automatic Data Processing, Inc.', 'ADP', NULL, NULL, NULL, NOW()),
('AppLovin Corporation', 'APP', NULL, NULL, NULL, NOW()),
('Salesforce, Inc.', 'CRM', NULL, NULL, NULL, NOW()),
('CrowdStrike Holdings, Inc.', 'CRWD', NULL, NULL, NULL, NOW()),
('Alphabet Inc.', 'GOOGL', NULL, NULL, NULL, NOW()),
('International Business Machines Corporation', 'IBM', NULL, NULL, NULL, NOW()),
('Intuit Inc.', 'INTU', NULL, NULL, NULL, NOW()),
('Meta Platforms, Inc.', 'META', NULL, NULL, NULL, NOW()),
('Microsoft Corporation', 'MSFT', NULL, NULL, NULL, NOW()),
('Cloudflare, Inc.', 'NET', NULL, NULL, NULL, NOW()),
('Netflix, Inc.', 'NFLX', NULL, NULL, NULL, NOW()),
('ServiceNow, Inc.', 'NOW', NULL, NULL, NULL, NOW()),
('Oracle Corporation', 'ORCL', NULL, NULL, NULL, NOW()),
('Palo Alto Networks, Inc.', 'PANW', NULL, NULL, NULL, NOW()),
('Palantir Technologies Inc.', 'PLTR', NULL, NULL, NULL, NOW()),
('SAP SE', 'SAP', NULL, NULL, NULL, NOW()),
('Shopify Inc.', 'SHOP', NULL, NULL, NULL, NOW()),
('Spotify Technology S.A', 'SPOT', NULL, NULL, NULL, NOW());

--Calculating SMA and creating trigger for it
create view full_detail as(
	with sma as(
		SELECT 
	    history_id,
		company_id,
	    price_date,
	    price_close,
	    AVG(price_close) OVER (
	        PARTITION BY company_id 
	        ORDER BY price_date 
	        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
	    ) AS sma30,
	    
	    AVG(price_close) OVER (
	        PARTITION BY company_id 
	        ORDER BY price_date 
	        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
	    ) AS sma60
	
		FROM stock_price_history
		ORDER BY company_id, price_date
	)
	
	SELECT 
	    company_id,
	    price_date,
	    price_close,
	    sma30,
	    sma60,
	    LAG(sma30) OVER (PARTITION BY company_id ORDER BY price_date) AS prev_sma30,
	    LAG(sma60) OVER (PARTITION BY company_id ORDER BY price_date) AS prev_sma60,
	    
	    CASE 
	        WHEN sma30 > sma60 AND LAG(sma30) OVER (PARTITION BY company_id ORDER BY price_date) <= LAG(sma60) OVER (PARTITION BY company_id ORDER BY price_date) 
	            THEN 'Golden Cross'
	        WHEN sma30 < sma60 AND LAG(sma30) OVER (PARTITION BY company_id ORDER BY price_date) >= LAG(sma60) OVER (PARTITION BY company_id ORDER BY price_date) 
	            THEN 'Death Cross'
	        ELSE NULL 
	    END AS crossover_signal
	
	FROM sma
	ORDER BY company_id, price_date
);

--Counting profits and losses if selling and buying during golden cross
CREATE or replace VIEW price_changes_golden as(
	WITH golden_crosses AS (
    SELECT
        company_id,
        price_date AS golden_date,
        price_close AS golden_price
    FROM stock_price_history
    WHERE crossover_signal = 'Golden Cross'
	),
	death_crosses AS (
	    SELECT
	        company_id,
	        price_date AS death_date,
	        price_close AS death_price
	    FROM stock_price_history
	    WHERE crossover_signal = 'Death Cross'
	),
	cross_pairs AS (
	    SELECT
	        g.company_id,
	        g.golden_date,
	        g.golden_price,
	        d.death_date,
	        d.death_price
	    FROM golden_crosses g
	    JOIN LATERAL (
	        SELECT *
	        FROM death_crosses d
	        WHERE d.company_id = g.company_id AND d.death_date > g.golden_date
	        ORDER BY d.death_date
	        LIMIT 1
	    ) d ON true
	)
	SELECT
	    company_id,
	    golden_date,
	    death_date,
	    golden_price,
	    death_price,
	    ROUND((death_price - golden_price), 2) AS price_change,
	    ROUND(((death_price - golden_price) / golden_price) * 100, 2) AS percent_change
	FROM cross_pairs
	ORDER BY company_id, golden_date
);

CREATE or replace VIEW price_changes_death as(
	WITH golden_crosses AS (
    SELECT
        company_id,
        price_date AS golden_date,
        price_close AS golden_price
    FROM stock_price_history
    WHERE crossover_signal = 'Golden Cross'
	),
	death_crosses AS (
	    SELECT
	        company_id,
	        price_date AS death_date,
	        price_close AS death_price
	    FROM stock_price_history
	    WHERE crossover_signal = 'Death Cross'
	),
	cross_pairs AS (
	    SELECT
			d.death_date,
	        d.death_price,
	        g.company_id,
	        g.golden_date,
	        g.golden_price
	    FROM death_crosses d
	    JOIN LATERAL (
	        SELECT *
	        FROM golden_crosses g
	        WHERE d.company_id = g.company_id AND d.death_date < g.golden_date
	        ORDER BY g.golden_date
	        LIMIT 1
	    ) g ON true
	)
	SELECT
	    company_id,
		death_date,
	    golden_date,
	    golden_price,
	    death_price,
	    ROUND((death_price - golden_price), 2) AS price_change,
	    ROUND(((death_price - golden_price) / golden_price) * 100, 2) AS percent_change
	FROM cross_pairs
	ORDER BY company_id, golden_date
);

create or replace function update_a_SMA_crossover()
	returns trigger
	language plpgsql
as 
$$
begin
	UPDATE stock_price_history AS s
	SET sma30 = fd.sma30,
	    sma60 = fd.sma60,
		crossover_signal = fd.crossover_signal
	FROM full_detail as fd
	WHERE s.company_id = fd.company_id
	AND s.price_date = fd.price_date;
	return new;
end;
$$;

create or replace function update_price_changes_golden()
	returns trigger
	language plpgsql
as 
$$
begin
	UPDATE stock_price_history AS s
	SET golden_cross_start_price = pcg.golden_price,
	    golden_cross_end_price = pcg.death_price,
		golden_cross_end_date = pcg.death_date,
		golden_cross_price_change = pcg.price_change,
		golden_cross_price_change_percentage = pcg.percent_change
	FROM price_changes_golden as pcg
	WHERE s.company_id = pcg.company_id
	AND s.price_date = pcg.golden_date;
	return new;
end;
$$;

create or replace function update_price_changes_death()
	returns trigger
	language plpgsql
as 
$$
begin
	UPDATE stock_price_history AS s
	SET death_cross_start_price = pcg.death_price,
	    death_cross_end_price = pcg.golden_price,
		death_cross_end_date = pcg.golden_date,
		death_cross_price_change = pcg.price_change,
		death_cross_price_change_percentage = pcg.percent_change
	FROM price_changes_death as pcg
	WHERE s.company_id = pcg.company_id
	AND s.price_date = pcg.death_date;
	return new;
end;
$$;

-- create trigger update_a_history_trigger
-- after insert or update on stock_price_history
-- for each row
-- execute function update_a_sma_crossover();

-- create trigger update_price_changes_golden_trigger
-- after insert or update on stock_price_history
-- for each row
-- execute function update_price_changes_golden();

-- create trigger update_price_changes_death_trigger
-- after insert or update on stock_price_history
-- for each row
-- execute function update_price_changes_death();