SELECT
	city as ds_city,
	city_ibge_code as cd_city_ibge,
	cast(`date` as date) as dt_date,
	epidemiological_week as cd_epidemiological_week,
	estimated_population as vl_estimated_population,
	estimated_population_2019 as vl_estimated_population_2019,
	case
		when is_last = False then 0
		else 1
	end as fl_is_last,
	case
		when is_repeated = False then 0
		else 1
	end as fl_is_repeated,
	last_available_confirmed as vl_last_available_confirmed,
	last_available_confirmed_per_100k_inhabitants,
	last_available_date as dt_last_available_date,
	last_available_death_rate as vl_last_available_death_rate,
	last_available_deaths as vl_last_available_deaths,
	order_for_place as cd_order_for_place,
	state as ds_state,
	new_confirmed as vl_new_confirmed,
	new_deaths as vl_new_deaths
FROM
	teste_dados.bronze_mysql_covid
WHERE 
	place_type = 'city';