SELECT
  `_id` as cd_multas,
  escopo_autuacao as ds_escopo_autuacao,
  mes as ds_mes,
  ano as cd_ano,
  uf as ds_uf,
  amparo_legal as ds_amparo_legal,
  descricao_infracao as ds_infracao,
  quantidade_autos as qt_autos
FROM
	teste_dados.bronze_mongodb_multas