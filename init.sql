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

