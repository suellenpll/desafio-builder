SELECT
	ROW_NUMBER() OVER(ORDER BY id_date) AS id_covid,
	id_date,
	fl_is_last,
	fl_is_repeated,
	vl_last_available_confirmed,
	last_available_confirmed_per_100k_inhabitants,
	dt_last_available_date,
	vl_last_available_death_rate,
	vl_last_available_deaths,
	cd_order_for_place,
	vl_new_confirmed,
	vl_new_deaths,
	id_city,
	id_state,
	id_city_ibge
FROM (
	SELECT
		gdd.id_date,
		fl_is_last,
		fl_is_repeated,
		vl_last_available_confirmed,
		last_available_confirmed_per_100k_inhabitants,
		dt_last_available_date,
		vl_last_available_death_rate,
		vl_last_available_deaths,
		cd_order_for_place,
		vl_new_confirmed,
		vl_new_deaths,
		gdc.id_city,
		gds.id_state,
		gdci.id_city_ibge
	FROM
		teste_dados.silver_covid_city scc
		LEFT JOIN teste_dados.gold_dim_state gds on scc.ds_state = gds.ds_state
		LEFT JOIN teste_dados.gold_dim_city gdc on scc.ds_city = gdc.ds_city
		LEFT JOIN teste_dados.gold_dim_city_ibge gdci on scc.cd_city_ibge = gdci.cd_city_ibge
		LEFT JOIN teste_dados.gold_dim_date gdd on scc.dt_date = gdd.dt_date 
	UNION 
		SELECT
		gdd.id_date,
		fl_is_last,
		fl_is_repeated,
		vl_last_available_confirmed,
		last_available_confirmed_per_100k_inhabitants,
		dt_last_available_date,
		vl_last_available_death_rate,
		vl_last_available_deaths,
		cd_order_for_place,
		vl_new_confirmed,
		vl_new_deaths,
		NULL,
		id_state,
		NULL
	FROM
		teste_dados.silver_covid_state scs
		LEFT JOIN teste_dados.gold_dim_state gds on scs.ds_state = gds.ds_state 
		LEFT JOIN teste_dados.gold_dim_date gdd on scs.dt_date = gdd.dt_date 
		) AS t 
ORDER BY id_date