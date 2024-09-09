# Indicium Academy

## Projeto de Infraestrutura de Dados e Dashboards Estratégicos

# Descrição do Projeto

Este projeto foi desenvolvido para a Adventure Works com o objetivo de implementar uma infraestrutura moderna de dados e criar dashboards estratégicos que auxiliem na tomada de decisões de diferentes áreas da empresa. A iniciativa busca aproveitar o poder dos dados para otimizar processos e melhorar o desempenho em diversas frentes, como vendas, planejamento de demanda e promoções.

# Objetivos do Projeto

* Implementar um data warehouse para centralizar e organizar os dados da empresa.
* Desenvolver dashboards estratégicos para fornecer insights valiosos e embasar decisões comerciais e operacionais.
* Demonstrar o valor da análise preditiva para o planejamento de demanda e outras áreas da empresa.
* Capacitar as equipes a tomarem decisões baseadas em dados, promovendo uma cultura data-driven na organização.

# Estrutura do Projeto

O projeto está organizado em fases, desde a coleta e integração dos dados até a criação de dashboards e geração de insights. Abaixo está a estrutura geral:

 1. Coleta e Integração de Dados
Para centralizar e processar os dados de forma eficiente, o pipeline utilizou o GitHub como principal fonte de dados.

2. Armazenamento em Data Warehouse (DW)
O armazenamento e processamento dos dados ocorreram no BigQuery (BQ), uma das soluções de data warehouse em nuvem mais utilizadas no MDS. A escolha por um DW em nuvem facilita a escalabilidade e oferece alta performance nas consultas, o que possibilita o processamento otimizado de grandes volumes de dados.

3. Transformação de Dados
Após o armazenamento no BigQuery, os dados brutos foram transformados usando o dbt, ferramenta que organiza, limpa e modela os dados de forma eficiente. A transformação é crucial, pois prepara os dados para análise, organizando-os em tabelas de fato e dimensões, realizando agregações e criando métricas intermediárias. O dbt também foi utilizado para documentar as tabelas de origem, criar staging tables e calcular as métricas necessárias.

4. Armazenamento dos Dados Transformados no BigQuery
Depois de transformados, os dados foram novamente armazenados no BigQuery, agora já organizados e prontos para consumo. Esse armazenamento otimizado garante que as consultas possam ser feitas rapidamente, permitindo que ferramentas de visualização acessem os dados de forma eficiente.

5. Visualização de Dados
A visualização dos dados foi realizada no Power BI (PBI), uma ferramenta robusta de business intelligence que permite a criação de dashboards interativos e relatórios intuitivos. O PBI auxilia na tomada de decisões baseadas em dados, permitindo que os insights extraídos do pipeline de dados sejam acessíveis e úteis para a empresa.

Vantagens do Modern Data Stack (MDS)
O pipeline foi construído dentro da filosofia do Modern Data Stack (MDS), proporcionando flexibilidade, escalabilidade e autonomia. Cada etapa do pipeline pode ser adaptada de acordo com as necessidades da empresa sem grandes investimentos ou mudanças na infraestrutura. Assim, é possível realizar ajustes pontuais no pipeline sem a necessidade de uma reformulação completa.

# Investimento Contínuo em Inovação:

Solução Preditiva: Com base no sucesso das promoções analisadas, recomenda-se investir em uma solução preditiva para o planejamento de demanda. Isso pode trazer ganhos significativos em eficiência e ajudar a otimizar as promoções, evitando comprometer a lucratividade. Implementar análises preditivas permitirá ajustar as campanhas de marketing de maneira mais estratégica, baseando-se em dados históricos e previsões.

Capacitação em BI e Análise de Dados: Para maximizar o retorno sobre o investimento em tecnologias de BI e análise de dados, é essencial treinar as equipes da AW no uso dessas ferramentas. Isso garantirá que os funcionários possam tomar decisões informadas baseadas nos insights extraídos dos dados. O treinamento deve incluir a utilização das ferramentas de visualização e análise para melhorar a tomada de decisões tanto na área comercial quanto nas demais áreas da empresa.

Estratégia de Promoções: Considerando os insights obtidos com o dashboard, recomenda-se uma abordagem mais estratégica para a aplicação de promoções. As promoções devem ser monitoradas e limitadas de acordo com seu impacto na receita. Identificar quais tipos de promoções funcionam melhor para cada categoria de produto ajudará a evitar descontos excessivos e a preservar a margem de lucro. Por exemplo, é importante evitar descontos superiores a 60% e focar em promoções mais eficazes para categorias de produtos com menor impacto na receita.
Monitoramento e Ajustes Contínuos:

Avaliação e Ajustes: Continuar monitorando o impacto das estratégias de vendas e promoções através dos dashboards e relatórios. Ajustar as táticas com base nos resultados e no feedback das áreas envolvidas garantirá que a empresa mantenha uma abordagem ágil e eficiente em suas operações e estratégias comerciais.


