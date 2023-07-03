SELECT
	ROW_NUMBER() OVER (ORDER BY ds_state) AS id_state,
    t.ds_state
FROM
    (
        SELECT DISTINCT ds_uf as ds_state FROM teste_dados.silver_multas
        UNION
        SELECT DISTINCT ds_state FROM teste_dados.silver_covid_city
        UNION
        SELECT DISTINCT ds_state FROM teste_dados.silver_covid_state
    ) AS t
ORDER BY ds_state;