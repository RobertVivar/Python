def delete_by_date(p_schema, p_table_name, p_delete_by, p_dates):
    query = query_delete = f"""
        SET NOCOUNT ON
        WHILE 1 = 1
        BEGIN
         DELETE TOP (10000) 
         FROM [{p_schema}].[{p_table_name}]
         WHERE [{p_delete_by}] >= '{p_dates[0]}' AND [{p_delete_by}] <= '{p_dates[1]}'
         IF @@rowcount < 1 BREAK;
        END """

    return query


def delete_by_truncate(p_schema, p_table_name,):
    query = query_delete = f"""
        SET NOCOUNT ON
        WHILE 1 = 1
        BEGIN
         DELETE TOP (10000) 
         FROM [{p_schema}].[{p_table_name}]
         IF @@rowcount < 1 BREAK;
        END """

    return query
