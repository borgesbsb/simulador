import streamlit as st
import numpy as np
import pandas as pd
import time

def insert_simulador(cursor, connection, nome):
    query = '''INSERT INTO SIMULADOR (NOME) VALUES (%s) '''
    tentativas = 0
    inserted = False
    while tentativas < 6:
        try:
            cursor.execute(query, (nome))
            resultado = cursor.fetchone()
            connection.commit ()
            inserted = True
            # print(resultado) # Se resultado = None Sinal que o insert foi realizado com sucesso
            break
        except Exception as e:
            resultado = f"Erro na hora da Inserção na tentativa {tentativas} reprocessando"
            # print(str(e))
            # print(resultado)

        tentativas += 1
    return inserted

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


def simulador(lengthcolumnscsv,first_run,dfInput, positionRef1, positionRef2, positionRef3):
    
    dfInput["ref"] = ""
    dfInput["sum"] = 0
    if first_run == 0 :
        
        dfInput.loc[positionRef1,"ref"] = "ref1"
        dfInput.loc[positionRef2,"ref"] = "ref2"
        dfInput.loc[positionRef3,"ref"] = "ref3"
    else:
        ref3_before_position = positionRef3 -1
        dfInput.loc[positionRef1,'ref'] = "ref1",
        dfInput.loc[positionRef2,'ref'] = "ref2",
        dfInput.loc[ref3_before_position,'ref'] = ""
        dfInput.loc[positionRef3,'ref'] = "ref3"
    
    
    count = 0
    #Substituir o nome das colunas que seja diferente de ref e sum
    sub_columns_hash = {}
    for index, value in enumerate(dfInput.columns.values[0:-2]):
        sub_columns_hash[value] = index

    sub_columns_hash
    dfInput.rename(columns = sub_columns_hash, inplace = True)
    #Identificar as colunas aonde tem valores nas posições de ref1 ref2 e ref3
    identificando_colunas_cinzas_df = dfInput[(dfInput["ref"] == "ref1") | (dfInput["ref"] == "ref2") | (dfInput["ref"] == "ref3")]
    
    drop_columns = []
    for column, value in enumerate(identificando_colunas_cinzas_df.sum(axis = 0)[:(identificando_colunas_cinzas_df.shape[1] - 2)]):
        if value == 0:
            drop_columns.append(column)

        #Df com as colunas que possuem valor
    dfInput = dfInput[identificando_colunas_cinzas_df.drop(columns = drop_columns).columns.values]
        # Atribuir os valores da soma para a coluna SUM
    numeric_columns = dfInput.columns.difference(["ref", "sum"])
    
    # Realizar a soma apenas das colunas numéricas
    dfInput["sum"] = dfInput[numeric_columns].sum(axis=1) 

        
    
        # Posição do ref3
    posicao_ref3 = dfInput[dfInput["ref"] == "ref3"].index[0]

        #Remover os valores abaixo do index onde está situado o ref3
    final_df = dfInput[dfInput.index > posicao_ref3]
        
    return final_df, posicao_ref3



def simulador_controller(cursor, connection, df, simulador_id):
    start_time = time.time()
    lengthcsv = df.shape[0]
    lengthcolumnscsv = df.shape[1]
    id_tabela_principal = 0
    latest_iteration = st.empty()
    bar = st.progress(0)
    
    # Update the progress bar with each iteration. 
    instanceSimulatorColumn = lengthcolumnscsv + 1
    total_iteracoes_acompanhamento = 0
    total_iteracoes = round(((lengthcsv - 2) * (lengthcsv - 1) * (lengthcsv)) / 4.2)
    
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
                total_iteracoes_acompanhamento += 1
                total_iteracoes_percentuais = round((total_iteracoes_acompanhamento / total_iteracoes) * 100)
                latest_iteration.text(f'Iteration {total_iteracoes_percentuais}')
                bar.progress(total_iteracoes_percentuais)
                dfInput = df.copy()
                contadorThirdFor += 1
                ref3 = ref2 + contadorThirdFor
                
                if ref3 >= lengthcsv:
                    break
                
                final_df, posicao_ref3 = simulador(lengthcolumnscsv, z, dfInput, ref1, ref2, ref3)
                colunasCinzas = ""
                print(f"ref1 {ref1} ref2 {ref2} ref3 {ref3} simulador_id {simulador_id} ")
                insert_tabela_principal(cursor, connection, simulador_id, ref1, ref2, ref3, colunasCinzas)
                id_tabela_principal = query_tabela_principal(cursor, connection)["ID"]
                print(f"id tabela principal : {id_tabela_principal}")
                bulkTotais = []
                
                for idx, total in enumerate(start=(posicao_ref3 + 1), iterable=(final_df["sum"].to_list())):
                    tempTotais = []
                    tempTotais.append(id_tabela_principal)
                    tempTotais.append(total)
                    tempTotais.append(idx)
                    bulkTotais.append(tempTotais)
                insert_totais(cursor, connection, bulkTotais)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Exibir o tempo de execução
    latest_iteration.text(f'Tempo de Execução {execution_time}')
    return "Todas Informações Inseridas"


def simulador_controller_continuacao(cursor, connection, df, simulador_id, ref1_inicial, ref2, ref3 ):
    lengthcsv = df.shape[0]
    lengthcolumnscsv = df.shape[1]
    id_tabela_principal = 0
    #Descomentar quando colocar no streamlit
    latest_iteration = st.empty()
    bar = st.progress(0)

    #Update the progress bar with each iteration. 
    instanceSimulatorColumn = lengthcolumnscsv + 1
    total_iteracoes_acompanhamento = 0
    total_iteracoes = round(((lengthcsv - 2) * (lengthcsv - 1) * (lengthcsv)) / 4.2)
    
    for x in range(ref1_inicial,(lengthcsv - 1)):
    #     print("oi")
        ref1 = x
        if x != ref1_inicial:
            ref2 = ref1  +  1
            ref3 = ref2 + 1
    #     print(f"x layers ref1: {ref1} ref2: {ref2} ref3:{ref3}")

        contadorSecondFor = 0
        for y in range(lengthcsv - 1):
    #         print("oi2")
            contadorSecondFor += 1
            if y != 0:
                
                if x != ref1_inicial:
                    ref2 = ref1 + contadorSecondFor
                    ref3 = ref2 + 1
                else:
                    ref2 = ref2 + 1
                    ref3 = ref2 + 1

#             print(f"y layers ref1: {ref1} ref2: {ref2} ref3:{ref3} contadorSecondFor: {contadorSecondFor}")
            if ref2 >= (lengthcsv -1):
                print("Break y")
                break

            contadorThirdFor = 0
            for z in range(lengthcsv):
                #Descomentar quando for para o streamlit
                total_iteracoes_acompanhamento +=1
                print(total_iteracoes_acompanhamento, total_iteracoes)
                total_iteracoes_percentuais = round((total_iteracoes_acompanhamento / total_iteracoes) * 100)
                latest_iteration.text(f'Iteration {total_iteracoes_percentuais}')
                bar.progress(total_iteracoes_percentuais)
                
                dfInput = df.copy()
                contadorThirdFor += 1
                if z != 0:
                    if x != ref1_inicial and y != 0 :
#                         print("AUI")
                        ref3 = ref2 + contadorThirdFor 
                    else:
#                         print("UI")
                        ref3 = ref3 + 1
                    if ref3 >= lengthcsv:
                        print("Break z")
                        break
                
                final_df, posicao_ref3 = simulador(lengthcolumnscsv,z,dfInput,ref1, ref2, ref3)
                
                colunasCinzas = ""
                print(f"ref1 {ref1} ref2 {ref2} ref3 {ref3} simulador_id {simulador_id} ")
                insert_tabela_principal(cursor, connection, simulador_id,ref1,ref2,ref3,colunasCinzas)
                id_tabela_principal = query_tabela_principal(cursor, connection)["ID"]
                print(f" id tabela principal : {id_tabela_principal}")
                bulkTotais = []
                # break
                for idx, total in enumerate(start = (posicao_ref3 +1), iterable = (final_df["sum"].to_list())):
    #         #         print(f" Indice : {idx} valor {total} ")
                    tempTotais = []
                    tempTotais.append(id_tabela_principal)
                    tempTotais.append(total)
                    tempTotais.append(idx)
                    bulkTotais.append(tempTotais)
                insert_totais(cursor, connection, bulkTotais)