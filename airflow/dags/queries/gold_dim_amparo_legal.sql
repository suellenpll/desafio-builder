SELECT
	ROW_NUMBER() OVER (ORDER BY ds_amparo_legal) AS id_amparo_legal,
    case when t.ds_amparo_legal = 'Null' then "Sem amparo legal" else ds_amparo_legal end ds_amparo_legal
FROM
    (
        SELECT DISTINCT ds_amparo_legal FROM teste_dados.silver_multas 
    ) AS t
ORDER BY ds_amparo_legal;