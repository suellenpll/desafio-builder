SELECT
	ROW_NUMBER() OVER (ORDER BY cd_city_ibge)as id_city_ibge,
    cd_city_ibge,
    id_city,
    vl_estimated_population,
    vl_estimated_population_2019
FROM
	(
		SELECT DISTINCT
			cd_city_ibge,
		    id_city,
		    vl_estimated_population,
		    vl_estimated_population_2019
		FROM
		    teste_dados.silver_covid_city sm
		    INNER JOIN teste_dados.gold_dim_city gdc on sm.ds_city = gdc.ds_city
	) AS t
ORDER BY cd_city_ibge;