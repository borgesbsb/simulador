def todos_simuladores(cursor, connection):
    query = '''select * from SIMULADOR '''
#     select * from resultados r left join tabela_principal t ON t.ID = r.tabela_principal_id where r.total = 5 and t.simulador_id = 43 ;
    tentativas = 0
    resultado = {}
    while tentativas < 6:
        try:
            cursor.execute(query)
            resultado = cursor.fetchall()
            connection.commit ()
            
            break
        except Exception as e:
            resultado = f"Erro na hora da busca na tentativa {tentativas} reprocessando"
            print(str(e))
            print(resultado)
        
        tentativas += 1
    return resultado

def query_resultado(cursor, connection, valor, simulador_id):
    
    # query = '''select * from resultados r left join tabela_principal t ON t.ID = r.tabela_principal_id where r.total = %s '''
    query = '''
    SELECT r.TOTAL, r.POSICAO AS LINHA, t.REF1, t.REF2, t.REF3
    FROM RESULTADOS r
    LEFT JOIN TABELA_PRINCIPAL t ON t.ID = r.TABELA_PRINCIPAL_ID
    WHERE r.TOTAL = %s AND t.SIMULADOR_ID = %s
'''
    tentativas = 0
    resultado = {}
    while tentativas < 6:
        try:
            cursor.execute(query, (valor, simulador_id) )
            resultado = cursor.fetchall()
            connection.commit ()
            
            break
        except Exception as e:
            resultado = f"Erro na hora da Busca de resultados na tentativa {tentativas} reprocessando"
            print(str(e))
            print(resultado)
        
        tentativas += 1
    return resultado

def count_registros(cursor, connection, simulador_id):
    query = '''select COUNT(*) from RESULTADOS r left join TABELA_PRINCIPAL t ON t.ID = r.tabela_principal_id where t.SIMULADOR_ID = %s'''
    tentativas = 0
    resultado = {}
    while tentativas < 6:
        try:
            cursor.execute(query, (simulador_id) )
            resultado = cursor.fetchall()
            connection.commit ()

            break
        except Exception as e:
            resultado = f"Erro na hora da Inserção na tentativa {tentativas} reprocessando"
            print(str(e))
    print(resultado)

    tentativas += 1
    return resultado

def select_last_iteration(cursor, connection, simulador_id):
    query = 'select * from TABELA_PRINCIPAL WHERE SIMULADOR_ID = %s order by id desc LIMIT 1 ;'
    tentativas = 0
    found = False
    while tentativas < 6:
        try:
            cursor.execute(query, (simulador_id))
            resultado = cursor.fetchone()
            connection.commit ()
            found = True
            break
        except Exception as e:
            resultado = f"Erro na hora da Inserção na tentativa {tentativas} reprocessando"
            print("Erro para selecionar o ID do simulador : ", str(e))
            # print(resultado)

        tentativas += 1
    return resultado 


def update_simulador_finalizado(cursor,connection,simulador_id):
    query = 'update  SIMULADOR  set SIMULACAO_FINALIZADA = true where id = %s ;'
    tentativas = 0
    found = False
    while tentativas < 6:
        try:
            cursor.execute(query, (simulador_id))
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