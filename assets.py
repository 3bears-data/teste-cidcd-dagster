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
    """Execucao baixa 4"""
    print("Executa baixa"
          )

@asset(group_name="gitGroup")
def getFromGit():
    """Atualiza os arquivos do grupo pipeline"""
    import git
    import os
    import shutil

    # URL do repositório do GitHub
    repo_url = 'https://github.com/3bears-data/teste-cidcd-dagster.git'

    # Caminho da pasta onde você deseja salvar o script
    dest_folder = '/opt/dagster/dagster_home/clone_git'

    # Nome do arquivo a ser baixado
    file_name = 'assets.py'

    try:
        shutil.rmtree(dest_folder, ignore_errors=True)
    except:
        None

    # Clonando o repositório
    repo = git.Repo.clone_from(repo_url, dest_folder)

    # Movendo o arquivo baixado para o diretório desejado
    src_file = os.path.join(dest_folder, file_name)
    dest_file = os.path.join(dest_folder, file_name)
    os.rename(src_file, dest_file)

    print('Download concluído!')

    # Caminho da pasta de destino
    dest_path = '/opt/dagster/dagster_home/dagster_home'

    # Caminho completo do arquivo baixado
    src_file = os.path.join(dest_folder, file_name)

    # Caminho completo do arquivo de destino
    dest_file = os.path.join(dest_path, file_name)

    # Copiar o arquivo para a pasta de destino
    shutil.copy(src_file, dest_file)

    print('Arquivo copiado para a pasta de destino com sucesso!')  
    
    return True

@asset(group_name="gitGroup")
def reloadPipeline(getFromGit):
    """Refresh no pipeline"""
    from dagster_graphql import DagsterGraphQLClient
    client = DagsterGraphQLClient("localhost", port_number=3000)   
    
    REPO_NAME = "dagster_home"
    reload_info: ReloadRepositoryLocationInfo = client.reload_repository_location(REPO_NAME)
    if reload_info.status == ReloadRepositoryLocationStatus.SUCCESS:
        print("Sucesso no reload")
    else:
        raise Exception(
            "Repository location reload failed because of a "
            f"{reload_info.failure_type} error: {reload_info.message}"
        )    
