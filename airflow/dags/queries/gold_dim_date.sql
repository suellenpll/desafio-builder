SELECT
    replace(dt_date,"-","") as id_date,
    dt_date,
    MONTH(dt_date)as cd_month,
    MONTHNAME(dt_date) as ds_month,
    YEAR(dt_date) as cd_year,
    concat(YEAR(dt_date),"-",IF(LENGTH(MONTH(dt_date))>1,MONTH(dt_date),concat("0",MONTH(dt_date)))) as ds_year_month,
    WEEKOFYEAR(dt_date)  as cd_week,
    CONCAT(YEAR(dt_date),"-",IF(LENGTH(WEEKOFYEAR(dt_date))>1,WEEKOFYEAR(dt_date),concat("0",WEEKOFYEAR(dt_date)))) as ds_year_week
FROM
    (
        SELECT DISTINCT dt_date FROM teste_dados.silver_covid_city
        UNION
        SELECT DISTINCT dt_date FROM teste_dados.silver_covid_state
        UNION
        SELECT
		CONCAT( 
		cd_ano,"-",
		CASE
			WHEN ds_mes = 'JANEIRO' THEN "01"
			WHEN ds_mes = 'FEVEREIRO' THEN "02"
			WHEN ds_mes = 'MARÇO' THEN "03"
			WHEN ds_mes = 'ABRIL' THEN "04"
			WHEN ds_mes = 'MAIO' THEN "05"
			WHEN ds_mes = 'JUNHO' THEN "06"
			WHEN ds_mes = 'JULHO' THEN "07"
			WHEN ds_mes = 'AGOSTO' THEN "08"
			WHEN ds_mes = 'SETEMBRO' THEN "09"
			WHEN ds_mes = 'OUTUBRO' THEN "10"
			WHEN ds_mes = 'NOVEMBRO' THEN "11"
			WHEN ds_mes = 'DEZEMBRO' THEN "12"
		END,"-01")
		FROM
			silver_multas sm
		    ) AS t
ORDER BY id_date;