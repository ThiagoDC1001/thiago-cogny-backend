DROP VIEW if exists ${schema:raw}.vw_population CASCADE;

CREATE OR REPLACE VIEW ${schema:raw}.vw_population AS
	SELECT SUM(items."Population") AS population FROM thiagodc1001.api_data,jsonb_to_recordset(thiagodc1001.api_data.doc_record -> 'data') AS items("Year" INT, "Population" INT) WHERE items."Year" BETWEEN 2018 AND 2020
;

