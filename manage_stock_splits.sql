BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 2),
	price_open = (price_open / 2),
	price_high = (price_high / 2),
	price_low = (price_low / 2)
	WHERE
	company_id = 2
	AND price_date < DATE '2000-10-25';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 2),
	price_open = (price_open / 2),
	price_high = (price_high / 2),
	price_low = (price_low / 2)
	WHERE
	company_id = 2
	AND price_date < DATE '2005-05-24';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 2.002),
	price_open = (price_open / 2.002),
	price_high = (price_high / 2.002),
	price_low = (price_low / 2.002)
	WHERE
	company_id = 7
	AND price_date < DATE '2014-04-03';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 1.0027455),
	price_open = (price_open / 1.0027455),
	price_high = (price_high / 1.0027455),
	price_low = (price_low / 1.0027455)
	WHERE
	company_id = 7
	AND price_date < DATE '2015-04-27';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 20),
	price_open = (price_open / 20),
	price_high = (price_high / 20),
	price_low = (price_low / 20)
	WHERE
	company_id = 7
	AND price_date < DATE '2022-07-18';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 2),
	price_open = (price_open / 2),
	price_high = (price_high / 2),
	price_low = (price_low / 2)
	WHERE
	company_id = 9
	AND price_date < DATE '2006-07-07';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 2),
	price_open = (price_open / 2),
	price_high = (price_high / 2),
	price_low = (price_low / 2)
	WHERE
	company_id = 11
	AND price_date < DATE '2003-02-18';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 2),
	price_open = (price_open / 2),
	price_high = (price_high / 2),
	price_low = (price_low / 2)
	WHERE
	company_id = 13
	AND price_date < DATE '2004-02-12';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 7),
	price_open = (price_open / 7),
	price_high = (price_high / 7),
	price_low = (price_low / 7)
	WHERE
	company_id = 13
	AND price_date < DATE '2015-07-15';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 2),
	price_open = (price_open / 2),
	price_high = (price_high / 2),
	price_low = (price_low / 2)
	WHERE
	company_id = 15
	AND price_date < DATE '2000-01-19';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 2),
	price_open = (price_open / 2),
	price_high = (price_high / 2),
	price_low = (price_low / 2)
	WHERE
	company_id = 15
	AND price_date < DATE '2000-10-13';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 3),
	price_open = (price_open / 3),
	price_high = (price_high / 3),
	price_low = (price_low / 3)
	WHERE
	company_id = 16
	AND price_date < DATE '2022-09-14';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 2),
	price_open = (price_open / 2),
	price_high = (price_high / 2),
	price_low = (price_low / 2)
	WHERE
	company_id = 16
	AND price_date < DATE '2024-12-16';
COMMIT;

BEGIN;
	UPDATE stock_price_history
	SET 
	price_close = (price_close / 10),
	price_open = (price_open / 10),
	price_high = (price_high / 10),
	price_low = (price_low / 10)
	WHERE
	company_id = 19
	AND price_date < DATE '2022-06-29';
COMMIT;