# desafio-builder

## Objetivo

O objetivo deste projeto é solucionar o desafio da vaga de DA ao criar um ETL utilizando Airflow. O projeto trata os dados de origem, fornecidos pela Builder, atraves das zonas/camadas de dados: Bronze, Silver e Gold. Construindo na camada Gold um DW em Star Schema e visualizando os dados no dashboard do POwer BI.

## Tecnologias Utilizadas

- Linguagem de programação: Python e SQL
- Ferramenta de ETL: Airflow
- Banco de dados: MySQl
- Controle de Versão: Github
- Vizualiação: Power BI

## Instalação e Execução

1. Clone este repositório.

2. Crie e ative um ambiente virtual:
```
python3 -m venv nome_do_ambiente  

source nome_do_ambiente/bin/activate
```
3. Instale as dependências:
```
pip install -r requirements.txt
```
4. Execute o aplicativo:
```
airflow standalone
```
5. Acesse a aplicação no navegador em http://localhost:8080.

## Conclusão

Esse projeto foi muito desafiador e gratificante, pois no geral, quando entramos em uma empresa o ambiente de dados já está configurado. Mas com esse desafio eu pude me aprofundar e estudar a implementação de ferramentas que eu utilizava. Indepedente do resultado, foi um ganho inestimavel e rico de conhecimento.