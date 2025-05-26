# Skills Importantes
1. Fundamentos (Spark e Engenharia de Dados)
2. Arquitetura do Spark
3. PySpark e SparkSQL
4. Processamento de Dados em Larga Escala
5. Streaming e Processamento Real-time
6. Integração de Dados
7. Arquitetura de Lakehouse
8. Performance Tunning

# As melhores Praticas (Resolve 70% dos problemas)

### Entrada
- Usar formatos otimizados para data analytics
- Otimize o particionamento de dados (Para resolver o skew e evitar small files que podem causar i/o excessivo)
- Utilize prune columns e predicate pushdown (select de colunas e filter de dados)
- Fazer broadcast join para datasets muito pequenos

### Transformers
- Evitar shuffles desnecessarios - Evitar wide transformations (Usar lambda)
- Evitar transformações ineficientes (UDFs) -> Usar Spark Native > Pandas UDF > Python UDF em ultimo caso!!
- Gerenciar a memoria para evitar spill e out of memory (cache de tabelas gigantes, collect e etc)
- Utilizar cache para dfs intermediarios (Quando tiver vários downstreams consumindo ele)
- Joins otimizados (broadcast) com hint

### Out
- Trabalhar com cargas full e insert (append sempre que possivel)
- Limpar versoes antigas do dataset
- Nao escrever small files -> Otimizar com repartition ou coalesce
- Utilizar formatos de arquivo eficientes (Delta e Iceberg)
- Particionamento de Dados (Liquid clustering no databricks)
- Usar o script size_estimator `jobs/melhores_praticas/app/src/size-estimator.py` para estimar o tamanho dele em memoria e a quantidade de particoes.


# Pattern para Desenvolvimento
- Organizacao de Codigo e Modularizacao
    - Divida por ingestao, transformacao e carga
    - Crie funcoes reutilizaveis para tasks comuns
- Gerenciamento de Configuracoes
    - Mantenha as configs em arquivos externos
    - Dividir as configs de dev e prod
    - Utilizar variaveis de ambiente para esconder dados sensiveis
- Logging e Monitoramento:
    - Salvar logs estruturados
    - Prometheus e Grafana para monitorar
- Técnicas de Otimização
    - Usar actions (show, counter, write) de forma estratégicas para triggar as computações (lazy)
    - Usar variáveis broadcast para datasets muito pequenos
    - Melhorar as partições para reduzir shuffle usando repartition ou coalesce.
- Geralmente usamos factory, builder e decorator.

# Validação de Dados e Checks de Qualidade
- Schema Enforcement
    - Levar em conta de onde o dado esta vindo (da bronze, silver, gold)
    - `jobs/melhores_praticas/app/src/yelp-dataset-sch-enforce.py`
    - Gatekeeper: Manter o controle do dado do jeito que queremos
    - A diferença de performance explicitando o schema ou colocando inferSchema é praticamente nula.  Para arquivos de lakehouse, geralmente usamos o inferSchema e deixamos pra forcar quando algo realmente for critico.
    - **Yara Data Profiling** (Funciona para datasets menores)
        - `jobs/melhores_praticas/app/src/yelp-dataset-yada-profiling.py`
        - Gera um html com um profiling dos dados
        - Da pra identificar skew se vc tiver uma coluna mal distribuida.
        - Para datasets muito grandes podemos usar as proprias estatisticas do pyspark
            - `jobs/melhores_praticas/app/src/yelp-dataset-large-dataset-analysis`

# Testes
- Testes Unitarios
- Testes de Integração
- O Pyspark tem cada vez mais desenvolvido funções de assert com pyspark.testing.
- `jobs/melhores_praticas/app/tests/test-yelp-dataset-unit-pytest.py`
- Deequ -> faz uma analise do profilling dos dados
    - `jobs/melhores_praticas/app/srcyelp-dataset-python-deequ`
- Chispa: permite varias validacoes e testes
- Quiin: traz varios metodos para validacao pra usar ao longo do codigo