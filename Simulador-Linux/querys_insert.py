import streamlit as st
import numpy as np
import pandas as pd
import time


def insert_simulador(cursor, connection, nome):
    # Verificar se já existe um simulador com o mesmo nome
    check_query = "SELECT ID FROM SIMULADOR WHERE NOME = %s"
    cursor.execute(check_query, (nome,))
    existing_simulador = cursor.fetchone()

    if existing_simulador:
        print(f"Já existe um simulador com o nome '{nome}'. A inserção não será realizada.")
        return False
    else:
        # Inserir o novo simulador se não existir outro com o mesmo nome
        insert_query = "INSERT INTO SIMULADOR (NOME) VALUES (%s)"
        cursor.execute(insert_query, (nome,))
        connection.commit()
        print(f"Simulador '{nome}' inserido com sucesso.")
        return True

def select_simulador_id(cursor, connection, nome):
    query = '''SELECT ID FROM SIMULADOR WHERE NOME = (%s)'''
    tentativas = 0
    found = False
    while tentativas < 6:
        try:
            cursor.execute(query, (nome))
            resultado = cursor.fetchone()
            connection.commit ()
            found = True
            # print(resultado) # Se resultado = None Sinal que o insert foi realizado com sucesso
            break
        except Exception as e:
            resultado = f"Erro na hora da Inserção na tentativa {tentativas} reprocessando"
            print("Erro para selecionar o ID do simulador : ", str(e))
            # print(resultado)

        tentativas += 1
    return resultado 


def insert_tabela_principal(cursor, connection, simulador_id,ref1,ref2,ref3,colunasCinzas):
    query = '''INSERT INTO TABELA_PRINCIPAL (SIMULADOR_ID, REF1,REF2,REF3, COLUNAS_CINZAS) VALUES (%s,%s,%s,%s,%s) '''
    tentativas = 0
    # print("Iniciando a Inserção da tabela principal")
    while tentativas < 6:
        try:
            cursor.execute(query, (simulador_id, (ref1), (ref2), (ref3),colunasCinzas))
            resultado = cursor.fetchone()
            connection.commit ()
            print(resultado) # Se resultado = None Sinal que o insert foi realizado com sucesso
            break
        except Exception as e:
            resultado = f"Erro na hora da Inserção na tentativa {tentativas} reprocessando"
            print("Erro na inserção da tabela_princial :",   str(e))
            # print(resultado)

        tentativas += 1
    # print(resultado)
    return resultado


def query_tabela_principal(cursor, connection):
    query = '''SELECT ID FROM TABELA_PRINCIPAL ORDER BY ID DESC'''
    tentativas = 0
    while tentativas < 6:
        try:
            cursor.execute(query)
            resultado = cursor.fetchone()
            connection.commit ()
            print(resultado) # Se resultado = None Sinal que o insert foi realizado com sucesso
            break
        except Exception as e:
            resultado = f"Erro na query tabela_principal na tentativa {tentativas} reprocessando"
            print(str(e))
            print(resultado)

        tentativas += 1
    return resultado

def insert_totais(cursor, connection, bulk_totais):
    
    query = '''INSERT INTO RESULTADOS (TABELA_PRINCIPAL_ID, TOTAL,POSICAO) VALUES (%s,%s,%s) '''
    tentativas = 0
    while tentativas < 6:
        try:
            cursor.executemany(query, bulk_totais)
#             resultado = cursor.fetchone()
            connection.commit ()
            print("Registros inseridos pelo bulk_totais") # Se resultado = None Sinal que o insert foi realizado com sucesso
            break
        except Exception as e:
            resultado = f"Erro na hora da Inserção dos totais tentativa {tentativas} reprocessando"
            print(str(e))
            print(resultado)

        tentativas += 1


# def simulador(lengthcolumnscsv, first_run, dfInput, positionRef1, positionRef2, positionRef3):
#     dfInput["ref"] = ""
#     dfInput["sum"] = 0

#     if first_run == 0:
#         dfInput.at[positionRef1, "ref"] = "ref1"
#         dfInput.at[positionRef2, "ref"] = "ref2"
#         dfInput.at[positionRef3, "ref"] = "ref3"
#     else:
#         ref3_before_position = positionRef3 - 1
#         dfInput.at[positionRef1, 'ref'] = "ref1"
#         dfInput.at[positionRef2, 'ref'] = "ref2"
#         dfInput.at[ref3_before_position, 'ref'] = ""
#         dfInput.at[positionRef3, 'ref'] = "ref3"

#     numeric_columns = dfInput.columns.difference(["ref", "sum"])

#     # Atribuição vetorizada do valor de "sum"
#     dfInput["sum"] = dfInput[numeric_columns].sum(axis=1)

#     posicao_ref3 = dfInput[dfInput["ref"] == "ref3"].index[0]

#     final_df = dfInput.loc[dfInput.index > posicao_ref3].copy()

#     return final_df, posicao_ref3


def simulador(lengthcolumnscsv, first_run, dfInput, positionRef1, positionRef2, positionRef3):
    dfInput["ref"] = ""
    dfInput["sum"] = 0
    if first_run == 0 :
        dfInput.loc[positionRef1, "ref"] = "ref1"
        dfInput.loc[positionRef2, "ref"] = "ref2"
        dfInput.loc[positionRef3, "ref"] = "ref3"
    else:
        ref3_before_position = positionRef3 - 1
        dfInput.loc[positionRef1, 'ref'] = "ref1"
        dfInput.loc[positionRef2, 'ref'] = "ref2"
        dfInput.loc[ref3_before_position, 'ref'] = ""
        dfInput.loc[positionRef3, 'ref'] = "ref3"

    count = 0
    sub_columns_hash = {}
    for index, value in enumerate(dfInput.columns.values[0:-2]):
        sub_columns_hash[value] = index

    dfInput.rename(columns=sub_columns_hash, inplace=True)

    identificando_colunas_cinzas_df = dfInput[(dfInput["ref"] == "ref1") | (dfInput["ref"] == "ref2") | (dfInput["ref"] == "ref3")]

    drop_columns = []
    for column, value in enumerate(identificando_colunas_cinzas_df.sum(axis=0)[:(identificando_colunas_cinzas_df.shape[1] - 2)]):
        if value == 0:
            drop_columns.append(column)

    dfInput = dfInput[identificando_colunas_cinzas_df.drop(columns=drop_columns).columns.values]

    numeric_columns = dfInput.columns.difference(["ref", "sum"])

    # Use .loc para atribuir valores diretamente ao DataFrame original
    dfInput.loc[:, "sum"] = dfInput[numeric_columns].sum(axis=1)

    posicao_ref3 = dfInput[dfInput["ref"] == "ref3"].index[0]

    final_df = dfInput[dfInput.index > posicao_ref3]

    return final_df, posicao_ref3





import time




# def simulador_controller(cursor, connection, df, simulador_id):
#     try:
#         start_time = time.time()

#         lengthcsv = df.shape[0]
#         lengthcolumnscsv = df.shape[1]
#         id_tabela_principal = 0
        
#         ref1_iteration = st.empty()
#         ref2_iteration = st.empty()
#         ref3_iteration = st.empty()
        
#         bar_ref1 = st.progress(0)
#         bar_ref2 = st.progress(0)
#         bar_ref3 = st.progress(0)

#         # Desativar restrições de chave estrangeira
#         cursor.execute("SET foreign_key_checks = 0")

#         # Inicie a transação fora do loop
#         connection.begin()

#         # Correção do cálculo total_iteracoes
        
#         # for ref1 in range(lengthcsv - 2):
#         #     for ref2 in range(ref1 + 1, lengthcsv - 1):
#         #         for ref3 in range(ref2 + 1, lengthcsv):
#         #             total_iteracoes += 1

#         # Preparar consultas de inserção fora do loop
#         insert_tabela_principal_query = '''INSERT INTO TABELA_PRINCIPAL (SIMULADOR_ID, REF1, REF2, REF3, COLUNAS_CINZAS) VALUES (%s, %s, %s, %s, %s)'''
#         insert_resultados_query = '''INSERT INTO RESULTADOS (TABELA_PRINCIPAL_ID, TOTAL, POSICAO) VALUES (%s, %s, %s)'''

#         # Consulta para inserir a posição da iteração
#         insert_posicao_query = '''INSERT INTO POSICAO_ITERACAO (SIMULADOR_ID, POSICAO) VALUES (%s, %s)'''

#         # Obter o ID da tabela principal fora do loop
#         query_last_id = '''SELECT ID FROM TABELA_PRINCIPAL ORDER BY ID DESC LIMIT 1'''
#         cursor.execute(query_last_id)
#         result_last_id = cursor.fetchone()
#         last_id = result_last_id["ID"] if result_last_id else 0

#         # Lista para armazenar os resultados
#         all_results = []

#         # Defina o intervalo de tempo desejado em segundos (15 minutos = 900 segundos)
#         intervalo_tempo = 15
#         tempo_anterior = time.time()
#         total_iteracoes_acompanhamento = 0

#         for ref1 in range(lengthcsv - 2):
#             for ref2 in range(ref1 + 1, lengthcsv - 1):
#                 for ref3 in range(ref2 + 1, lengthcsv):
#                     # total_iteracoes_acompanhamento += 1
                    
#                     total_iteracoes_percentuais_ref1 = round(((ref1 + 1) / (lengthcsv - 2)) * 100)
#                     ref1_iteration.text(f'REF1   {ref1 + 1}  de   {lengthcsv}')
#                     bar_ref1.progress(total_iteracoes_percentuais_ref1)

#                     total_iteracoes_percentuais_ref2 = round(((ref2 + 1) / (lengthcsv - 1)) * 100)
#                     ref2_iteration.text(f'REF2   {ref2 + 1}  de   {lengthcsv}')
#                     bar_ref2.progress(total_iteracoes_percentuais_ref2)

#                     total_iteracoes_percentuais_ref3 = round(((ref3 + 1 ) / (lengthcsv)) * 100)
#                     ref3_iteration.text(f'REF3   {ref3 + 1}  de   {lengthcsv}')
#                     bar_ref3.progress(total_iteracoes_percentuais_ref3)

                    
                    
#                     dfInput = df.copy()

#                     final_df, posicao_ref3 = simulador(lengthcolumnscsv, ref3, dfInput, ref1, ref2, ref3)
#                     colunasCinzas = ""
#                     print(f"ref1 {ref1} ref2 {ref2} ref3 {ref3} simulador_id {simulador_id} ")

#                     # Armazenar resultados em memória
#                     all_results.append((simulador_id, ref1, ref2, ref3, colunasCinzas, final_df["sum"].to_list()))

#                     # Verificar se o intervalo de tempo foi atingido
#                     tempo_atual = time.time()
#                     if tempo_atual - tempo_anterior >= intervalo_tempo:
#                         # Inserir resultados no banco de dados em uma única transação
#                         for result in all_results:
#                             cursor.execute(insert_tabela_principal_query, result[:-1])
#                             id_tabela_principal = last_id + 1
#                             last_id = id_tabela_principal
#                             bulkTotais = [(id_tabela_principal, total, idx) for idx, total in enumerate(result[-1], start=1)]
#                             cursor.executemany(insert_resultados_query, bulkTotais)

#                         # Inserir a posição da iteração
#                         cursor.execute(insert_posicao_query, (simulador_id, total_iteracoes_acompanhamento))

#                         # Commit da transação
#                         connection.commit()

#                         # Resetar a lista de resultados e o tempo anterior
#                         all_results = []
#                         tempo_anterior = tempo_atual

#         # Inserir resultados finais no banco de dados, se houver algum restante
#         # if all_results:
#         #     for result in all_results:
#         #         cursor.execute(insert_tabela_principal_query, result[:-1])
#         #         id_tabela_principal = last_id + 1
#         #         last_id = id_tabela_principal
#         #         bulkTotais = [(id_tabela_principal, total, idx) for idx, total in enumerate(result[-1], start=1)]
#         #         cursor.executemany(insert_resultados_query, bulkTotais)

#         # Ativar restrições de chave estrangeira
#         cursor.execute("SET foreign_key_checks = 1")

#         # Commit da transação ao final das inserções
#         # connection.commit()

#         # Calcular o tempo de execução
#         end_time = time.time()
#         execution_time = end_time - start_time

#         # Exibir o tempo de execução
#         # latest_iteration.text(f'Tempo de Execução {execution_time}')

#         return "Todas Informações Inseridas"

#     except Exception as e:
#         # Em caso de erro, faça rollback da transação
#         connection.rollback()
#         print("Erro durante a inserção:", str(e))
#         return "Erro durante a inserção"


def simulador_controller(cursor, connection, df, simulador_id):
    try:

        lengthcsv = df.shape[0]
        lengthcolumnscsv = df.shape[1]
        id_tabela_principal = 0
        
        # bar = st.progress(0)
        
        ref1_iteration = st.empty()
        ref2_iteration = st.empty()
        ref3_iteration = st.empty()
        
        bar_ref1 = st.progress(0)
        bar_ref2 = st.progress(0)
        bar_ref3 = st.progress(0)

        # Desativar restrições de chave estrangeira
        # cursor.execute("SET foreign_key_checks = 0")

        # Inicie a transação
        connection.begin()

        

        # Preparar a consulta de inserção da tabela principal fora do loop
        insert_tabela_principal_query = '''INSERT INTO TABELA_PRINCIPAL (SIMULADOR_ID, REF1, REF2, REF3, COLUNAS_CINZAS) VALUES (%s, %s, %s, %s, %s)'''

        # Preparar a consulta de inserção em lote para RESULTADOS
        insert_resultados_query = '''INSERT INTO RESULTADOS (TABELA_PRINCIPAL_ID, TOTAL, POSICAO) VALUES (%s, %s, %s)'''


        for x in range(1, (lengthcsv - 1), 1):
            ref1 = x
            ref2 = ref1 + 1
            ref3 = ref2 + 1
            contadorSecondFor = 0

            for y in range(lengthcsv - 1):
                contadorSecondFor += 1
                ref2 = ref1 + contadorSecondFor
                ref3 = ref2 + 1
                contadorThirdFor = 0

                if ref2 >= (lengthcsv - 1):
                    break

                for z in range(lengthcsv):
                    
                    total_iteracoes_percentuais_ref1 = round(((ref1+1) / (lengthcsv - 2)) * 100)
                    ref1_iteration.text(f'REF1   {ref1}  de   {lengthcsv}')
                    bar_ref1.progress(total_iteracoes_percentuais_ref1)

                    total_iteracoes_percentuais_ref2 = round(((ref2+1) / (lengthcsv - 1)) * 100)
                    ref2_iteration.text(f'REF2   {ref2}  de   {lengthcsv}')
                    bar_ref2.progress(total_iteracoes_percentuais_ref2)

                    total_iteracoes_percentuais_ref3 = round(((ref3+1) / (lengthcsv)) * 100)
                    ref3_iteration.text(f'REF3   {ref3}  de   {lengthcsv}')
                    bar_ref3.progress(total_iteracoes_percentuais_ref3)
                    

                    dfInput = df.copy()
                    contadorThirdFor += 1
                    ref3 = ref2 + contadorThirdFor

                    if ref3 >= lengthcsv:
                        break

                    final_df, posicao_ref3 = simulador(lengthcolumnscsv, z, dfInput, ref1, ref2, ref3)
                    colunasCinzas = ""
                    print(f"ref1 {ref1} ref2 {ref2} ref3 {ref3} simulador_id {simulador_id} ")

                    # Inserir na tabela principal
                    cursor.execute(insert_tabela_principal_query, (simulador_id, ref1, ref2, ref3, colunasCinzas))
                    resultado = cursor.fetchone()
                    print(resultado)  # Print do resultado da inserção
                           

                    # Obter o ID da tabela principal
                    query = '''SELECT ID FROM TABELA_PRINCIPAL ORDER BY ID DESC LIMIT 1'''
                    
                    cursor.execute(query)
                    resultado = cursor.fetchone()
                    id_tabela_principal = resultado["ID"]
                            

                    bulkTotais = []

                    for idx, total in enumerate(start=(posicao_ref3 + 1), iterable=(final_df["sum"].to_list())):
                        tempTotais = [id_tabela_principal, total, idx]
                        bulkTotais.append(tempTotais)
                    
                        # Inserir os totais em lote
                    cursor.executemany(insert_resultados_query, bulkTotais)
                        # print("Registros inseridos pelo bulk_totais")  # Print dos registros inseridos
                    connection.commit()
                # Ativar restrições de chave estrangeira
            # cursor.execute("SET foreign_key_checks = 1")
            
       
        return "Todas Informações Inseridas"

    except Exception as e:
        # Em caso de erro, faça rollback da transação
        connection.rollback()
        print("Erro durante a inserção:", str(e))
        return "Erro durante a inserção"



def simulador_controller_continuacao(cursor, connection, df, simulador_id, ref1_inicial, ref2_inicial, ref3_inicial):
    try:
        
        lengthcsv = df.shape[0]
        lengthcolumnscsv = df.shape[1]
        id_tabela_principal = 0
        
        ref1_iteration = st.empty()
        ref2_iteration = st.empty()
        ref3_iteration = st.empty()
        
        bar_ref1 = st.progress(0)
        bar_ref2 = st.progress(0)
        bar_ref3 = st.progress(0)
        
        connection.begin()
        
        # Preparar consultas de inserção fora do loop
        insert_tabela_principal_query = '''INSERT INTO TABELA_PRINCIPAL (SIMULADOR_ID, REF1, REF2, REF3, COLUNAS_CINZAS) VALUES (%s, %s, %s, %s, %s)'''
        insert_resultados_query = '''INSERT INTO RESULTADOS (TABELA_PRINCIPAL_ID, TOTAL, POSICAO) VALUES (%s, %s, %s)'''
        
        loop_inicial = True
        
        
        for x in range(ref1_inicial, (lengthcsv - 1), 1):
            ref1 = x
            ref2 = ref1 + 1
            ref3 = ref2 + 1
            contadorSecondFor = 0
            
            for y in range(lengthcsv - 1):
                contadorSecondFor += 1
                ref2 = ref1 + contadorSecondFor
                ref3 = ref2 + 1
                contadorThirdFor = 0
                
                if ref2 >= (lengthcsv - 1):
                    break
                
                for z in range(lengthcsv):
                    
                    if ref1 == ref1_inicial and ref2 == ref2_inicial and ref3 == ref3_inicial:
                        loop_inicial = False
                    
                    if loop_inicial:
                        ref1 = ref1_inicial
                        ref2 = ref2_inicial
                        ref3 = ref3_inicial
                        contadorSecondFor = ref2_inicial - ref1_inicial
                        loop_inicial = False
                        
                    
                    
                    total_iteracoes_percentuais_ref1 = round(((ref1) / (lengthcsv - 2)) * 100)
                    ref1_iteration.text(f'REF1   {ref1}  de   {lengthcsv}')
                    bar_ref1.progress(total_iteracoes_percentuais_ref1)

                    total_iteracoes_percentuais_ref2 = round(((ref2) / (lengthcsv - 1)) * 100)
                    ref2_iteration.text(f'REF2   {ref2}  de   {lengthcsv}')
                    bar_ref2.progress(total_iteracoes_percentuais_ref2)

                    total_iteracoes_percentuais_ref3 = round(((ref3) / (lengthcsv)) * 100)
                    ref3_iteration.text(f'REF3   {ref3}  de   {lengthcsv}')
                    bar_ref3.progress(total_iteracoes_percentuais_ref3)
                    

                    dfInput = df.copy()
                    contadorThirdFor += 1
                    ref3 = ref2 + contadorThirdFor

                    if ref3 >= lengthcsv:
                        break

                    final_df, posicao_ref3 = simulador(lengthcolumnscsv, z, dfInput, ref1, ref2, ref3)
                    colunasCinzas = ""
                    print(f"ref1 {ref1} ref2 {ref2} ref3 {ref3} simulador_id {simulador_id} ")

                    # Inserir na tabela principal
                    cursor.execute(insert_tabela_principal_query, (simulador_id, ref1, ref2, ref3, colunasCinzas))
                    resultado = cursor.fetchone()
                    print(resultado)  # Print do resultado da inserção
                           

                    # Obter o ID da tabela principal
                    query = '''SELECT ID FROM TABELA_PRINCIPAL ORDER BY ID DESC LIMIT 1'''
                    
                    cursor.execute(query)
                    resultado = cursor.fetchone()
                    id_tabela_principal = resultado["ID"]
                    
                    bulkTotais = []

                    for idx, total in enumerate(start=(posicao_ref3 + 1), iterable=(final_df["sum"].to_list())):
                        tempTotais = [id_tabela_principal, total, idx]
                        bulkTotais.append(tempTotais)
                    
                        # Inserir os totais em lote
                    cursor.executemany(insert_resultados_query, bulkTotais)
                        # print("Registros inseridos pelo bulk_totais")  # Print dos registros inseridos
                    connection.commit()
                
            
            
       
        return "Todas Informações Inseridas"
    
    except Exception as e:
        # Em caso de erro, faça rollback da transação
        connection.rollback()
        print("Erro durante a inserção:", str(e))
        return "Erro durante a inserção"
        
        
    





# def simulador_controller_continuacao(cursor, connection, df, simulador_id, ref1_inicial, ref2, ref3):
#     try:
#         start_time = time.time()

#         lengthcsv = df.shape[0]
#         lengthcolumnscsv = df.shape[1]
#         id_tabela_principal = 0
#         latest_iteration = st.empty()
        
#         ref1_iteration = st.empty()
#         ref2_iteration = st.empty()
#         ref3_iteration = st.empty()
        
        
#         bar_ref1 = st.progress(0)
#         bar_ref2 = st.progress(0)
#         bar_ref3 = st.progress(0)

#         # Desativar restrições de chave estrangeira
#         cursor.execute("SET foreign_key_checks = 0")

#         # Inicie a transação fora do loop
#         connection.begin()

#         total_iteracoes_acompanhamento = ref1_inicial
#         total_iteracoes = (lengthcsv - 3)
        
       
        
        

#         # Preparar consultas de inserção fora do loop
#         insert_tabela_principal_query = '''INSERT INTO TABELA_PRINCIPAL (SIMULADOR_ID, REF1, REF2, REF3, COLUNAS_CINZAS) VALUES (%s, %s, %s, %s, %s)'''
#         insert_resultados_query = '''INSERT INTO RESULTADOS (TABELA_PRINCIPAL_ID, TOTAL, POSICAO) VALUES (%s, %s, %s)'''

#         # Atualizar a posição da iteração no banco de dados
#         update_posicao_query = '''UPDATE POSICAO_ITERACAO SET POSICAO = %s WHERE SIMULADOR_ID = %s'''

        
#         # Consulta para inserir a posição da iteração
#         insert_posicao_query = '''INSERT INTO POSICAO_ITERACAO (SIMULADOR_ID, POSICAO) VALUES (%s, %s)'''

#         # Obter o ID da tabela principal fora do loop
#         query_last_id = '''SELECT ID FROM TABELA_PRINCIPAL ORDER BY ID DESC LIMIT 1'''
#         cursor.execute(query_last_id)
#         result_last_id = cursor.fetchone()
#         last_id = result_last_id["ID"] if result_last_id else 0
        
        
#         # Lista para armazenar os resultados
#         all_results = []

#         # Defina o intervalo de tempo desejado em segundos (25 minutos = 1500 segundos)
#         intervalo_tempo = 15
#         tempo_anterior = time.time()

#         for x in range(ref1_inicial, (lengthcsv - 1), 1):
#             ref1 = x
#             ref2 = ref1 + 1
#             ref3 = ref2 + 1
#             contadorSecondFor = 0

#             for y in range(lengthcsv - 1):
#                 contadorSecondFor += 1
#                 ref2 = ref1 + contadorSecondFor
#                 ref3 = ref2 + 1
#                 contadorThirdFor = 0

#                 if ref2 >= (lengthcsv - 1):
#                     break

#                 for z in range(lengthcsv):
#                     print(ref1, total_iteracoes)
                    
#                     total_iteracoes_percentuais_ref1 = round(((ref1 + 1) / (lengthcsv - 2)) * 100)
#                     ref1_iteration.text(f'REF1   {ref1 + 1}  de   {lengthcsv}')
#                     bar_ref1.progress(total_iteracoes_percentuais_ref1)

#                     total_iteracoes_percentuais_ref2 = round(((ref2 + 1) / (lengthcsv - 1)) * 100)
#                     ref2_iteration.text(f'REF2   {ref2 + 1}  de   {lengthcsv}')
#                     bar_ref2.progress(total_iteracoes_percentuais_ref2)

#                     total_iteracoes_percentuais_ref3 = round(((ref3 + 1 ) / (lengthcsv)) * 100)
#                     ref3_iteration.text(f'REF3   {ref3 + 1}  de   {lengthcsv}')
#                     bar_ref3.progress(total_iteracoes_percentuais_ref3)

#                     dfInput = df.copy()
#                     contadorThirdFor += 1
#                     ref3 = ref2 + contadorThirdFor

#                     if ref3 >= lengthcsv:
#                         break

#                     final_df, posicao_ref3 = simulador(lengthcolumnscsv, z, dfInput, ref1, ref2, ref3)
#                     colunasCinzas = ""
#                     print(f"ref1 {ref1} ref2 {ref2} ref3 {ref3} simulador_id {simulador_id} ")

#                     # Armazenar resultados em memória
#                     all_results.append((simulador_id, ref1, ref2, ref3, colunasCinzas, final_df["sum"].to_list()))

#                     # Verificar se o intervalo de tempo foi atingido
#                     tempo_atual = time.time()
#                     if tempo_atual - tempo_anterior >= intervalo_tempo:
#                         # Inserir resultados no banco de dados em uma única transação
#                         for result in all_results:
#                             cursor.execute(insert_tabela_principal_query, result[:-1])
#                             id_tabela_principal = last_id + 1
#                             last_id = id_tabela_principal
#                             bulkTotais = [(id_tabela_principal, total, idx) for idx, total in enumerate(result[-1], start=1)]
#                             cursor.executemany(insert_resultados_query, bulkTotais)

#                         # Atualizar a posição da iteração
#                         cursor.execute(update_posicao_query, (total_iteracoes_acompanhamento, simulador_id))

#                         connection.commit()

#                         # Resetar a lista de resultados e o tempo anterior
#                         all_results = []
#                         tempo_anterior = tempo_atual

#         # Inserir resultados finais no banco de dados, se houver algum restante
#         # if all_results:
#         #     for result in all_results:
#         #         cursor.execute(insert_tabela_principal_query, result[:-1])
#         #         id_tabela_principal = last_id + 1
#         #         last_id = id_tabela_principal
#         #         bulkTotais = [(id_tabela_principal, total, idx) for idx, total in enumerate(result[-1], start=1)]
#         #         cursor.executemany(insert_resultados_query, bulkTotais)

#         # Ativar restrições de chave estrangeira
#         cursor.execute("SET foreign_key_checks = 1")

#         # Commit da transação ao final das inserções
#         connection.commit()

#         # Calcular o tempo de execução
#         end_time = time.time()
#         execution_time = end_time - start_time

#         # Exibir o tempo de execução
#         latest_iteration.text(f'Tempo de Execução {execution_time}')

#         return "Todas Informações Inseridas"

#     except Exception as e:
#         # Em caso de erro, faça rollback da transação
#         connection.rollback()
#         print("Erro durante a inserção:", str(e))
#         return "Erro durante a inserção"