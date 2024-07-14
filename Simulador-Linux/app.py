#Senha do mysql
senha_banco = "B12905629g" #<<< mude esse valor entre as aspas

#Senha de proteção da aplicação, para criar e deletar simulações.
senha_protecao = "abc"#<<< mude esse valor entre as aspas


#Bibliotecas
import streamlit as st
import numpy as np
import pandas as pd
import pymysql.cursors
import xlrd
from querys_delete import delete_all, query_general
from querys_resultados import todos_simuladores, query_resultado, count_registros, select_last_iteration, update_simulador_finalizado
from querys_insert import insert_simulador, select_simulador_id, insert_tabela_principal, query_tabela_principal, insert_totais, simulador, simulador_controller, simulador_controller_continuacao
import os 


# def encontrar_repeticoes_consecutivas(df, coluna, num_repeticoes):
#     repeticoes_consecutivas = []
#     i = 0
#     df_repeticoes = pd.DataFrame()
#     while i < len(df)-num_repeticoes:
#         current_value = df[coluna][i] 
#         # Contar repetições consecutivas em num_repeticoes
#         temp = []
#         # temp.append(current_value)
#         procurar_repeticao = True
#         for j in range(num_repeticoes-1):
#             # print("Analisando", current_value, "na linha",i,"com", df[coluna][i+j+1], "na linha", i+j+1, "de",j, "repetições ")
#             if  current_value != df["REF3"].loc[i+j+1]:
#                 # print("Não é igual")
#                 procurar_repeticao = False
                
        
#         if procurar_repeticao == True:
#             for j in range(num_repeticoes):
#                 repeticoes_consecutivas.append(i+j)
#             i = i+num_repeticoes
#         else:
#             i = i+1
        
      
#     return repeticoes_consecutivas

def encontrar_repeticoes_consecutivas(df, coluna, num_repeticoes):
    repeticoes_consecutivas = []
    i = 0
    while i < len(df)-num_repeticoes:
        current_value_ref3 = df[coluna][i] 
        current_value_ref2 = df["REF2"][i]
        current_value_ref1 = df["REF1"][i]
        
        temp = [i]  # Inicia a lista temporária com o índice atual
        procurar_repeticao = True
        for j in range(num_repeticoes-1):
            if current_value_ref3 != df["REF3"].iloc[i+j+1] or current_value_ref2 != df["REF2"].iloc[i+j+1] or current_value_ref1 != df["REF1"].iloc[i+j+1] :
                procurar_repeticao = False
                break  # Se alguma condição não for atendida, interrompe o loop
        
        if procurar_repeticao and i + num_repeticoes < len(df):
            #verifica sequencia posterior para evitar repetições
            if current_value_ref3 == df["REF3"].iloc[i+num_repeticoes] and current_value_ref2 == df["REF2"].iloc[i+num_repeticoes] and current_value_ref1 == df["REF1"].iloc[i+num_repeticoes]:
                procurar_repeticao = False
        
        if procurar_repeticao:
            # Adiciona as linhas consecutivas à lista temporária
            for j in range(1, num_repeticoes):
                temp.append(i+j)
            # Adiciona a lista temporária à lista principal
            repeticoes_consecutivas.extend(temp)
            i = i + num_repeticoes
        else:
            i = i + 1
    
    return repeticoes_consecutivas

# def encontrar_repeticoes_consecutivas(df, coluna, num_repeticoes):
#     repeticoes_consecutivas = []
#     i = 0
#     while i <= len(df) - num_repeticoes:
#         current_value_ref3 = df[coluna][i] 
#         current_value_ref2 = df["REF2"][i]
#         current_value_ref1 = df["REF1"][i]
        
#         temp = [i]  # Inicia a lista temporária com o índice atual
#         procurar_repeticao = True
#         for j in range(num_repeticoes):
#             if i + j >= len(df) or current_value_ref3 != df[coluna].iloc[i+j] or \
#                current_value_ref2 != df["REF2"].iloc[i+j] or \
#                current_value_ref1 != df["REF1"].iloc[i+j]:
#                 procurar_repeticao = False
#                 break  # Se alguma condição não for atendida, interrompe o loop
        
#         # Verifica se há uma repetição subsequente, indicando uma sequência maior
#         if procurar_repeticao and i + num_repeticoes < len(df):
#             if current_value_ref3 == df[coluna].iloc[i+num_repeticoes] or \
#                current_value_ref2 == df["REF2"].iloc[i+num_repeticoes] or \
#                current_value_ref1 == df["REF1"].iloc[i+num_repeticoes]:
#                 procurar_repeticao = False
        
#         if procurar_repeticao:
#             # Adiciona as linhas consecutivas à lista temporária
#             repeticoes_consecutivas.extend(temp + list(range(i+1, i+num_repeticoes)))
#             i += num_repeticoes  # Avança o índice para além da sequência encontrada
#         else:
#             i += 1
    
#     return repeticoes_consecutivas





#Variaveis globais
print("------------------------------------- Reiniciou --------------------------------------")
simulador_id = 0
tentativas = 0
simulador_iniciado = True
validacao_simulador = ""



# Connect to the databas
conexao = ""
while tentativas < 6:
    try:

        connection = pymysql.connect(host="127.0.0.1",
                                     port = 3306,
                                     user='root',
                                     passwd= senha_banco,
                                     database='simuladorFinal',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        cursor = connection.cursor()
        conexao = "Conexão Com Database realizada com Sucesso!"
        st.write(conexao)    
        print(conexao)
        break
    except Exception as e:
        
        print(str(e))
        conexao = f"Não foi possível realizar conexão com Database após {tentativas} tentativas tente mais tarde"
        st.write(conexao)
    
    tentativas +=1




st.sidebar.markdown("## Escolha uma das funcionalidade abaixo ")

# delete = st.sidebar.checkbox("Delete Simuladores")
# search = st.sidebar.checkbox("Busca de Informações")
# upload = st.sidebar.checkbox("Faça uma nova Simulação ")
funcionalidade = st.sidebar.selectbox('Escolha uma das funcionalidades:', ['Pagina Inicial','Delete Simuladores','Busca de Informações','Faça uma nova Simulação', 'Finalize uma Simulação Interrompida'])
# print(f"Upload : {upload} ")
# print(f"Search : {search} ")
# print(f"Delete : {delete}")


if funcionalidade ==  'Faça uma nova Simulação' :
    
    st.markdown("""# Uploader 
    Página responsável pelo upload dos arquivos csv, para iniciar a simulação
    Você deve: \n
        1) Digite a senha de seguraça da sua aplicação  \n
        2) Faça o upload do arquivo csv ou xlsx \n
        3) Escolha o nome do seu simulador \n        
        4) Uma vez confirmado o arquivo aperte o botão "Inicialize o Simulador" \n
        5) Acompanhe o progresso da execução na barra de acompanhamento  
    """)
    # 4) Defina se o simulador fará uso do 'ref4' \n
    # search = False
    password = st.text_input("1) Qual a senha de proteção da sua aplicação ? ", type = 'password')

    # st.set_option('deprecation.showfileUploaderEncoding', False)



    if password == senha_protecao:

        uploaded_file = st.file_uploader("2) Faça o upload do arquivo CSV/XLSX")
        # uploaded_file = st.file_uploader("1) Faça o upload do arquivo XLSX", type="xlsx")

        if uploaded_file is not None:
            # print("DF")
            # print(uploaded_file.name)
            if uploaded_file.name.endswith(".csv"):
                df = pd.read_csv(uploaded_file, header = None)
            else :
                df = pd.read_excel(uploaded_file)
            df.index += 1
            df.columns = df.columns.astype(str)
            # df = pd.read_excel("ex.planilha.xlsx", sheet_name=None)
            # df = df["Plan1"].drop(columns = ["Unnamed: 0"])
            # df = df.head(10)
	    
            st.write(df)
            

            nome_simulador = st.text_input("3) Como você deseja nomear o seu simulador ? ")

            if len(nome_simulador) > 0 and simulador_id == 0 :
                print("Inserindo Simulador")
                insert_simulador(cursor, connection, nome_simulador)
                
                simulador_id = select_simulador_id(cursor, connection, nome_simulador)["ID"]
                
                if simulador_id > 0:
                    st.write(f"Simulador {nome_simulador} criado com sucesso")
                    df.to_parquet(f'simuladores_nao_finalizados/{nome_simulador}.parquet')
                    st.write(f"Simulador ID = {simulador_id}")
                    
                    if st.checkbox('4) Inicialize o Simulador'):
                        simulador_iniciado = True
                        st.write("5 ) Simulador iniciado aguarde a mensagem de finalização")
                        ultima_iteracao = select_last_iteration(cursor, connection, simulador_id)
                        
                        if ultima_iteracao == None:
                            print("None comecando do 0")
                            simulador_controller(cursor, connection, df, simulador_id)
                            st.write("Simulador Finalizado")
                            update_simulador_finalizado(cursor,connection,simulador_id)
                            os.remove(f'simuladores_nao_finalizados/{nome_simulador}.parquet')
                # tipo_ref = st.selectbox('Escolha se será uma simulacao com REF3 ou REF4 :', ["REF3", "REF4"])

elif funcionalidade == 'Busca de Informações' :
    st.markdown("""# Busca de Resultados 
    ## Página responsável pela análise das informações geradas pelo Simulador 
    Você deve: \n
        1) Digitar o valor que deseja análisar \n
        2) Digitar o ID do simulador que deseja análisar \n
        3) Marcar a caixinha que pergunta se os parametros (número do relatório e ID do simulador) já estão estabelicidos" \n
        4) Análisar os resultados gerados  
    """)
    # upload = False
#     st.markdown("## Análise dos Simuladores")
    "\n"
    "\n"
    "\n"
    st.markdown("### Nome e identificador dos Simuladores presentes no banco de dados")
    "\n"
#     todos_simuladores(cursor, connection)
    df_todos_simuladores = pd.DataFrame(todos_simuladores(cursor, connection))
    print(df_todos_simuladores)
    if df_todos_simuladores.empty:
        st.write("Não existem simuladores no banco de dados")
    else:
        df_todos_simuladores["SIMULACAO_FINALIZADA"] = df_todos_simuladores["SIMULACAO_FINALIZADA"].apply(lambda x: "SIM" if x == 1 else "NÃO")
        df_todos_simuladores[["ID", "NOME", "SIMULACAO_FINALIZADA"]]
    # valor = st.number_input("1) Digite o número que você deseja obter o relatório", min_value = 1, step = 1, format = "%i" )
    
    simulador_id = st.number_input("2) Digite o número do ID do simulador que vôcê deseja obter o relatório", min_value = 1, step = 1, format = "%i" )
    valor = st.number_input("1) Digite o número que você deseja obter o relatório", min_value = 0.0)
    valor = abs(valor)
    "\n"
    "\n"
    if st.checkbox('3) Se os parametros já estão estabelecidos clique aqui'):
        numero_registros = count_registros(cursor, connection , simulador_id)[0]['COUNT(*)']
        
        st.write(f"O número de registros do simulador {simulador_id} é {numero_registros}")
        resultados = query_resultado(cursor, connection, valor, simulador_id)
        
        # Correção: Criar o DataFrame corretamente
        df_resultados = pd.DataFrame.from_records(resultados)
        df_resultados
        ######Repetição
        repeticao = st.number_input("4) Digite o número de repetições, em seguida clique Enter ",min_value = 2, step = 1,format = "%i")
        # filtro = df_resultados[df_resultados["REF3"] == repeticao]
        # filtro
        valores_repetidos_consecutivos = encontrar_repeticoes_consecutivas(df_resultados, 'REF3', repeticao)
        linhas_filtradas = df_resultados.loc[valores_repetidos_consecutivos]
        linhas_filtradas
        
   

elif funcionalidade == 'Delete Simuladores' :
    
    st.markdown("""# Delete os Simuladores 
    Página responsável por deletar todas as informações referentes ao simulador escolhido 
    Você deve: \n
        
        1) Digitar o ID do Simulador que deseja DELETAR \n
        2) Marque a caixinha que pergunta se você tem CERTEZA que deseja deletar as informações do Simulador em questão \n
        3) Coloque a senha de segurança da sua aplicação \n
        4) Análise a mensagem colocar a senha de segurança, você será informado se o Simulador foi deletado ou não.  
    """)
    
    st.markdown("### Nome e identificador dos Simuladores presentes no banco de dados")
    "\n"
#     todos_simuladores(cursor, connection)
    df_todos_simuladores = pd.DataFrame(todos_simuladores(cursor, connection))
    
    if df_todos_simuladores.empty:
        st.write("Não existem simuladores no banco de dados")
    else:
        df_todos_simuladores[["ID", "NOME", "SIMULACAO_FINALIZADA"]]
    simulador_id = st.number_input("1) Digite o número do ID do simulador que vôcê deseja deletar", min_value = 1, step = 1, format = "%i" )
    if st.checkbox(f'2) Você tem certeza que deja deletar o Simulador de ID = {simulador_id} ?' ):
        password = st.text_input("3) Qual a senha de proteção da sua aplicação ? ", type = 'password')
        if password == senha_protecao:
    
            st.write(f"4) Deletando as Informações do Simulador {simulador_id} aguarde a mensagem final")
            st.write(delete_all(simulador_id, cursor, connection))
            st.empty()

elif funcionalidade == 'Finalize uma Simulação Interrompida':
    st.markdown("""# Continue sua Simulação 
    ## Página responsável por finalizar simulações iniciadas 
    Você deve: \n
        1) Digitar o ID do simulador interrompido \n
        2) Aguarde a mensagem de simulação finalizada \n
    """)
    ids = []
    df_todos_simuladores = pd.DataFrame(todos_simuladores(cursor, connection))
    if df_todos_simuladores.empty:
        st.write("Não existem simuladores no banco de dados")
    else:
        df_todos_simuladores["SIMULACAO_FINALIZADA"] = df_todos_simuladores["SIMULACAO_FINALIZADA"].apply(lambda x: "SIM" if x == 1 else "NÃO")
        df_todos_simuladores[["ID", "NOME", "SIMULACAO_FINALIZADA"]]
        ids = df_todos_simuladores[df_todos_simuladores["SIMULACAO_FINALIZADA"] == "NÃO"].ID.tolist()
    
    if len(ids) != 0:
        simulador_id = st.selectbox('Escolha o ID do simulador para dar continuidade as simulações:', ids)
        simulador_id = int(simulador_id)
        
        if st.checkbox('4) Inicialize o Simulador'):
            nome_simulador = df_todos_simuladores[df_todos_simuladores['ID'] == simulador_id]['NOME'].tolist()[0]
            file_path = f'simuladores_nao_finalizados/{nome_simulador}.parquet'
            
            
            if os.path.exists(file_path):
                df = pd.read_parquet(file_path, engine='pyarrow')
                simulador_iniciado = True
                st.write("5) Simulador iniciado, aguarde a mensagem de finalização")
                
                ultima_iteracao = select_last_iteration(cursor, connection, simulador_id)
                
                if ultima_iteracao is None:
                    print("Começando do zero")
                    # simulador_controller(cursor, connection, df, simulador_id)
                else:
                    print("Recomeçando de onde parou")
                    print(ultima_iteracao)
                    ref1_inicial = ultima_iteracao['REF1']
                    ref2 = ultima_iteracao['REF2']
                    ref3 = ultima_iteracao['REF3']
                    simulador_controller_continuacao(cursor, connection, df, simulador_id, ref1_inicial, ref2, ref3)
                
                st.write("Simulador Finalizado")
                # update_simulador_finalizado(cursor, connection, simulador_id)
                # os.remove(file_path)
            
        
else:
    st.markdown("""# Página Inicial 
    Escolha qual das funcionalidades no canto esquerdo da tela você deseja executar
    Você pode: \n

        1) Deletar Simuladores existentes \n
        2) Buscar Informações de Simuladores existentes  \n
        3) Criar uma nova simulação.  \n
        4) Continuar uma simulação interrompida \n
        5) Retornar a pagina inicial
    """)
