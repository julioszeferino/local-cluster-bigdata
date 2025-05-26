# Cria Imagem Spark

# Imagem do SO usada como base
FROM python:3.11-bullseye as spark-base

# Atualiza o SO e instala pacotes
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    sudo \
    curl \
    vim \
    nano \
    unzip \
    rsync \
    openjdk-11-jdk \
    build-essential \
    software-properties-common \
    ssh \
    dos2unix && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Variáveis de ambiente
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

# Cria as pastas
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download do arquivo de binários do Spark
RUN curl https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz -o spark-3.5.0-bin-hadoop3.tgz \
 && tar xvzf spark-3.5.0-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.5.0-bin-hadoop3.tgz

# Prepara o ambiente com PySpark
FROM spark-base as pyspark

# Instala as dependências Python
ADD requirements/requirements.txt .
RUN pip3 install -r requirements.txt

# Mais variáveis de ambiente
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# Copia o arquivo de configuração do Spark para a imagem
ADD conf/spark-defaults.conf "$SPARK_HOME/conf"
ADD conf/spark-env.sh "$SPARK_HOME/conf"

RUN dos2unix $SPARK_HOME/conf/spark-env.sh
RUN dos2unix $SPARK_HOME/conf/spark-defaults.conf

# jars
ADD jars/* "$SPARK_HOME/jars/"

RUN mkdir -p /var/log/spark

RUN mkdir -p /opt/spark/logs

# Permissões
RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

# Variável PYTHONPATH
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Copia o script de inicialização dos serviços para a imagem
ADD entrypoint.sh .

# Ajusta o privilégio
RUN dos2unix entrypoint.sh && chmod +x entrypoint.sh

RUN ls -la /opt/spark/conf && ls -la /opt/spark/jars && ls -la requirements.txt && ls -la entrypoint.sh

RUN cat entrypoint.sh

# Executa o script quando inicializar um container
ENTRYPOINT ["./entrypoint.sh"]

