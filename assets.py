from dagster import asset, get_dagster_logger, Output, MetadataValue # import the `dagster` library

@asset(group_name="pipelineGroup")
def topo():
    """Execucao primaria"""
    print("Executa topo")

    return True

@asset(group_name="pipelineGroup")
def mediano(topo):
    """Execucao mediana"""
    print("Executa mediana")

    return True

@asset(group_name="pipelineGroup")
def baixo(mediano):
    """Execucao baixa 3"""
    print("Executa baixa"
          )

@asset(group_name="gitGroup")
def getFromGit():
    """Atualiza os arquivos do grupo pipeline"""

    print("Update files")
