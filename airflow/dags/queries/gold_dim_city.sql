SELECT
    ROW_NUMBER() OVER (ORDER BY ds_city) AS id_city,
    ds_city AS ds_city,
    gds.id_state 
FROM
    (
    SELECT DISTINCT ds_city, ds_state
    FROM teste_dados.silver_covid_city
    ) AS t
    INNER JOIN teste_dados.gold_dim_state gds 
    ON t.ds_state = gds.ds_state 
ORDER BY id_city;