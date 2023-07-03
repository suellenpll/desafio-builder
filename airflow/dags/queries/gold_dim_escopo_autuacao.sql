SELECT
	ROW_NUMBER() OVER (ORDER BY ds_escopo_autuacao) AS id_escopo_autuacao,
    t.ds_escopo_autuacao
FROM
    (
        SELECT DISTINCT ds_escopo_autuacao FROM teste_dados.silver_multas
    ) AS t
ORDER BY ds_escopo_autuacao;