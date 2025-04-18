from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Sample data
test_data = [
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"ACTIVITY_ID","COLUMNORDINAL":1.0,"DATATYPE":"NUMBER","MAXLENGTH":22.0,"PRECISION":6.0,"SCALE":0.0,"NULLABLE":0.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"ACTIVITY_NM","COLUMNORDINAL":2.0,"DATATYPE":"VARCHAR2","MAXLENGTH":250.0,"NULLABLE":1.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"DESCP","COLUMNORDINAL":3.0,"DATATYPE":"VARCHAR2","MAXLENGTH":2000.0,"NULLABLE":1.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"ACTIVITY_TYP","COLUMNORDINAL":4.0,"DATATYPE":"VARCHAR2","MAXLENGTH":25.0,"NULLABLE":1.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"PERFORMER_ID","COLUMNORDINAL":5.0,"DATATYPE":"VARCHAR2","MAXLENGTH":50.0,"NULLABLE":1.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"EXECUTION_MODE","COLUMNORDINAL":6.0,"DATATYPE":"VARCHAR2","MAXLENGTH":3.0,"NULLABLE":1.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"EDIT_FLG","COLUMNORDINAL":7.0,"DATATYPE":"VARCHAR2","MAXLENGTH":1.0,"NULLABLE":0.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"REQUIRED_FLG","COLUMNORDINAL":8.0,"DATATYPE":"VARCHAR2","MAXLENGTH":1.0,"NULLABLE":1.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"ACTIVITY_ROLE_NM","COLUMNORDINAL":9.0,"DATATYPE":"VARCHAR2","MAXLENGTH":20.0,"NULLABLE":1.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"CREATE_DTT","COLUMNORDINAL":10.0,"DATATYPE":"DATE","MAXLENGTH":7.0,"NULLABLE":0.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"CREATE_USER_ID","COLUMNORDINAL":11.0,"DATATYPE":"VARCHAR2","MAXLENGTH":8.0,"NULLABLE":0.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"LAST_UPDATE_DTT","COLUMNORDINAL":12.0,"DATATYPE":"DATE","MAXLENGTH":7.0,"NULLABLE":0.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"LAST_UPDATE_USER_ID","COLUMNORDINAL":13.0,"DATATYPE":"VARCHAR2","MAXLENGTH":8.0,"NULLABLE":0.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"TPA_ADMIN_ACTVTY_FLG","COLUMNORDINAL":14.0,"DATATYPE":"VARCHAR2","MAXLENGTH":1.0,"NULLABLE":0.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"LETTER_GOAL_FILTER_CD","COLUMNORDINAL":15.0,"DATATYPE":"VARCHAR2","MAXLENGTH":20.0,"NULLABLE":0.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"CFG_DMN_VRSN_ID","COLUMNORDINAL":16.0,"DATATYPE":"VARCHAR2","MAXLENGTH":16.0,"NULLABLE":1.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"CREATED_OS_USER_ID","COLUMNORDINAL":17.0,"DATATYPE":"VARCHAR2","MAXLENGTH":30.0,"NULLABLE":0.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"UPDATED_OS_USER_ID","COLUMNORDINAL":18.0,"DATATYPE":"VARCHAR2","MAXLENGTH":30.0,"NULLABLE":0.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"CREATED_APP_USER_ID","COLUMNORDINAL":19.0,"DATATYPE":"VARCHAR2","MAXLENGTH":40.0,"NULLABLE":1.0},
    {"SCHEMANAME":"CIS","TABLENAME":"CFG_ACTIVITY","COLUMNNAME":"UPDATED_APP_USER_ID","COLUMNORDINAL":20.0,"DATATYPE":"VARCHAR2","MAXLENGTH":40.0,"NULLABLE":1.0}
]

# Function to get SQL and PySpark schema
def get_schema_from_list(raw_list: list):
    type_mapping = {
        'NUMBER': (IntegerType(), 'INT'),
        'VARCHAR2': (StringType(), 'STRING'),
        'DATE': (DateType(), 'DATE'),
        'CHAR': (StringType(), 'STRING'),
        'FLOAT': (DoubleType(), 'FLOAT')
    }

    struct_fields = []
    sql_fields = []

    for col in sorted(raw_list, key=lambda x: x["COLUMNORDINAL"]):
        col_name = col["COLUMNNAME"]
        data_type = col["DATATYPE"].upper()
        nullable = bool(col["NULLABLE"]) if col["NULLABLE"] is not None else True

        spark_type, sql_type = type_mapping.get(data_type, (StringType(), 'STRING'))

        struct_fields.append(StructField(col_name, spark_type, nullable))
        sql_fields.append(f"{col_name} {sql_type}")

    df_schema = StructType(struct_fields)
    sql_schema = ', '.join(sql_fields)

    return {"sql_schema": sql_schema, "df_schema": df_schema}

# Call the function
schema_result = get_schema_from_list(test_data)

# Print outputs
print("🔹 SQL Schema:\n")
print(schema_result["sql_schema"])

print("\n🔹 PySpark DataFrame Schema:\n")
print(schema_result["df_schema"])
