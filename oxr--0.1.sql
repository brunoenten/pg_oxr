-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION oxr" to load this file. \quit


-- Get config
CREATE FUNCTION oxr.get_config(key text) RETURNS text
    LANGUAGE sql STABLE
    AS $_$

  WITH oxr_config AS (
    --- From https://github.com/PostgREST/postgrest/blob/main/src/PostgREST/Config/Database.hs
    with
      role_setting as (
        select setdatabase, unnest(setconfig) as setting from pg_catalog.pg_db_role_setting
        where setrole = current_user::regrole::oid
          and setdatabase in (0, (select oid from pg_catalog.pg_database where datname = current_catalog))
      ),
      kv_settings as (
        select setdatabase, split_part(setting, '=', 1) as k, split_part(setting, '=', 2) as value from role_setting
        where setting like 'oxr.%'
      )
      select distinct on (key) replace(k, 'oxr.', '') as key, value
      from kv_settings
      order by key, setdatabase desc
  )

  SELECT "value" FROM oxr_config WHERE "key"=$1;
$_$;

COMMENT ON FUNCTION oxr.get_config(key text) IS 'Get value from oxr customized option from current role';

-- Set config
CREATE FUNCTION oxr.set_config(_key text, _value text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
  BEGIN
    EXECUTE format('ALTER ROLE %I SET oxr.%s TO %L', current_user, _key, _value);
  END;
$_$;

COMMENT ON FUNCTION oxr.set_config(_key text, _value text) IS 'Set value of oxr customized option to current role';

-- Get latest rate
CREATE OR REPLACE FUNCTION oxr.get_latest_rate(base_currency char(3), price_currency char(3)) RETURNS numeric
    LANGUAGE plpgsql
    AS $$
DECLARE
	app_id text;
	res_body json;
	res_status integer;
	req_url text;
	req_body text;
	rate numeric;
BEGIN
	app_id = oxr.get_config('app_id');
	IF app_id IS NULL THEN
		RAISE 'Please set app_id first by calling oxr.set_app_id(YOUR_APP_ID)';
	END IF;

	req_url = 'https://openexchangerates.org/api/latest.json?'
		|| urlencode(jsonb_build_object(	'app_id', app_id,
											'base', base_currency,
											'symbols', price_currency
	));

	SELECT status, content FROM http_get(req_url)
		INTO res_status, res_body;
	IF res_status != 200 THEN
		RAISE 'Error while retrieving rates: %', res_body::text;
		RETURN NULL;
	END IF;

	rate =  CAST(res_body#>>ARRAY['rates', price_currency] AS numeric);

	RETURN rate;
END;
$$;

COMMENT ON FUNCTION oxr.get_latest_rate(base_currency char(3), price_currency char(3)) IS 'Get latest rate from a pair of currencies';



-- Table: oxr.historical_rates
CREATE TABLE oxr.historical_rates
(
    base_currency character(3) COLLATE pg_catalog."default" NOT NULL,
    price_currency character(3) COLLATE pg_catalog."default" NOT NULL,
    price_on date NOT NULL,
    rate numeric NOT NULL
)

COMMENT ON TABLE oxr.historical_rates IS 'Cache storage for historical rates';

-- Get historical rate
CREATE OR REPLACE FUNCTION oxr.get_historical_rate(base_currency char(3), price_currency char(3), price_on date) RETURNS numeric
    LANGUAGE plpgsql
    AS $$
DECLARE
	app_id text;
	res_body json;
	res_status integer;
	req_url text;
	req_body text;
	output_rate numeric;
BEGIN
	-- Cache lookup
	output_rate = rate FROM oxr.historical_rates WHERE historical_rates.base_currency=get_historical_rate.base_currency AND historical_rates.price_currency=get_historical_rate.price_currency AND historical_rates.price_on=get_historical_rate.price_on LIMIT 1;
	IF output_rate IS NOT NULL THEN
		RETURN output_rate;
	END IF;

	RAISE NOTICE 'Cache miss';
	app_id = oxr.get_config('app_id');
	IF app_id IS NULL THEN
		RAISE 'Please set app_id first by calling oxr.set_app_id(YOUR_APP_ID)';
	END IF;

	req_url = format('https://openexchangerates.org/api/historical/%s.json?', price_on)
		|| urlencode(jsonb_build_object(	'app_id', app_id,
											'base', base_currency,
											'symbols', price_currency
	));

	SELECT status, content FROM http_get(req_url)
		INTO res_status, res_body;
	IF res_status != 200 THEN
		RAISE 'Error while retrieving rates: %', res_body::text;
		RETURN NULL;
	END IF;

	output_rate = CAST(res_body#>>ARRAY['rates', price_currency] AS numeric);

	INSERT INTO oxr.historical_rates VALUES (base_currency, price_currency, price_on, output_rate), (price_currency, base_currency, price_on, 1/output_rate);

	RETURN output_rate;
END;
$$;

COMMENT ON FUNCTION oxr.oxr.get_historical_rate(base_currency char(3), price_currency char(3), price_from date) IS 'Get historical rate from a pair of currencies on a given date';

-- Set app_id
CREATE FUNCTION oxr.set_app_id(app_id text) RETURNS void
	LANGUAGE sql
	AS $$
SELECT oxr.set_config('app_id', app_id);
$$;

COMMENT ON FUNCTION oxr.set_app_id(app_id text) IS 'Stores OpenExchangeRates app ID into calling role custom data.'


