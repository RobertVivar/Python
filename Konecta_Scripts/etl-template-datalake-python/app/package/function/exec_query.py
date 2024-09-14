def execute_query(cn, query):
    try:
        cursor = cn.cursor()
        cursor.execute(query)
        cn.commit()
        print('execute data, records...Ok')
        cursor.close()
        del cursor
        cn.close()
    except Exception as error:
        print(error)
