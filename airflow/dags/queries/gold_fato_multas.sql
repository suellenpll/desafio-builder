SELECT
    ROW_NUMBER() OVER (ORDER BY cd_multas) AS id_multas,
    cd_multas,
    qt_autos,
    id_escopo_autuacao,
    CASE
        WHEN id_amparo_legal IS NULL THEN (
            SELECT id_amparo_legal FROM teste_dados.gold_dim_amparo_legal WHERE ds_amparo_legal = 'Sem amparo legal'
        )
        ELSE id_amparo_legal
    END AS id_amparo_legal,
    id_infracao,
    id_state,
    CONCAT( 
		cd_ano,
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
		END,"01") as id_date
FROM
    teste_dados.silver_multas sm
LEFT JOIN teste_dados.gold_dim_escopo_autuacao gdea ON sm.ds_escopo_autuacao = gdea.ds_escopo_autuacao
LEFT JOIN teste_dados.gold_dim_amparo_legal gdal ON sm.ds_amparo_legal = gdal.ds_amparo_legal
LEFT JOIN teste_dados.gold_dim_infracao gdi ON sm.ds_infracao = gdi.ds_infracao
LEFT JOIN teste_dados.gold_dim_state gds ON sm.ds_uf = gds.ds_state;