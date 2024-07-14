def delete_all(simulador_id, cursor, connection):
    try:
        # Desativar restrições de chave estrangeira temporariamente para garantir exclusões em cascata
        cursor.execute("SET foreign_key_checks = 0")

        # Excluir da tabela RESULTADOS
        cursor.execute("DELETE FROM RESULTADOS WHERE TABELA_PRINCIPAL_ID IN (SELECT ID FROM TABELA_PRINCIPAL WHERE SIMULADOR_ID = %s)", (simulador_id,))
        print("Registros deletados da tabela RESULTADOS")

        
        # Excluir da tabela TABELA_PRINCIPAL
        cursor.execute("DELETE FROM TABELA_PRINCIPAL WHERE SIMULADOR_ID = %s", (simulador_id,))
        print("Registros deletados da tabela TABELA_PRINCIPAL")

        # Excluir do simulador
        cursor.execute("DELETE FROM SIMULADOR WHERE ID = %s", (simulador_id,))
        print("Registro deletado da tabela SIMULADOR")

        # Confirmar as alterações no banco de dados
        connection.commit()
        
        #optmize e analyze em todas as tabelas
        cursor.execute("OPTIMIZE TABLE RESULTADOS")
        cursor.execute("OPTIMIZE TABLE TABELA_PRINCIPAL")
        cursor.execute("OPTIMIZE TABLE SIMULADOR")
        cursor.execute("ANALYZE TABLE RESULTADOS")
        cursor.execute("ANALYZE TABLE TABELA_PRINCIPAL")
        cursor.execute("ANALYZE TABLE SIMULADOR")
        
        #close session
        cursor.close()
        
        

        return f"Todos os registros relacionados ao SIMULADOR_ID {simulador_id} foram deletados com sucesso."

    except Exception as e:
        # Em caso de erro, desfazer alterações e retornar mensagem de erro
        connection.rollback()
        return f"Erro ao deletar registros: {str(e)}"

    finally:
        # Restaurar restrições de chave estrangeira
        cursor.execute("SET foreign_key_checks = 1")
    
def query_general(query, cursor, connection, parameters):
    
    tentativas = 0
    resultado = {}
    cursor.execute(query, parameters)
    resultado = cursor.fetchall()
    connection.commit ()
    
    return resultado