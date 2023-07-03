SELECT
	ROW_NUMBER() OVER (ORDER BY ds_infracao) AS id_infracao,
    t.ds_infracao
FROM
    (
        SELECT DISTINCT ds_infracao FROM teste_dados.silver_multas
    ) AS t
ORDER BY ds_infracao;