from textwrap import dedent

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from adhoc.scdtype2 import SCDType2
from airflow.operators.python import PythonOperator
"""
### ETL DAG Tutorial Documentation
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""

source_dict = {
    "db_type": "oracle",
    "connection": "dwh_oracle",
    "table_name": "STG_LDLD_THONG_TIN_DOAN_VIEN",
    "schema_name": "DWH_STG",
}
target_dict = {
    "db_type": "oracle",
    "connection": "dwh_oracle",
    "table_name": "DIM_LDLD_THONG_TIN_DOAN_VIEN",
    "schema_name": "DWH_DIM",
}
map_column = {
    "MA_DOAN_VIEN": "MA_DOAN_VIEN",
    "MA_THE_DOAN_VIEN": "MA_THE_DOAN_VIEN",
    "TEN_DOAN_VIEN": "TEN_DOAN_VIEN",
    "GIOI_TINH": "GIOI_TINH",
    "NGAY_SINH": "NGAY_SINH",
    "QUE_QUAN": "QUE_QUAN",
    "SO_CMND": "SO_CMND",
    "CHUC_VU": "CHUC_VU",
    "NGAY_VAO_CD": "NGAY_VAO_CD",
    "MA_CONG_DOAN_CS": "MA_CONG_DOAN_CS",
    "VI_TRI_CONG_VIEC": "VI_TRI_CONG_VIEC",
    "NAM_VAO_LAM_VIEC": "NAM_VAO_LAM_VIEC"
}
# columns changed must is in columns target table
columns_changed = [
    "GIOI_TINH", "NGAY_SINH", "QUE_QUAN", "SO_CMND", "CHUC_VU", "NGAY_VAO_CD",
    "MA_CONG_DOAN_CS", "VI_TRI_CONG_VIEC", "NAM_VAO_LAM_VIEC"
]

scdtype2 = SCDType2(
    source_dict=source_dict,
    target_dict=target_dict,
    map_column=map_column,
    key_column="MA_DOAN_VIEN",
    columns_changed=columns_changed
)

# [END import_module]

# [START instantiate_dag]
with DAG(
    'etl_dim_thong_tin_doan_vien',
    default_args={'retries': 0},
    description='ETL DAG DIM etl_dim_thong_tin_doan_vien',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl', 'dim', 'tuan'],
) as dag:

    dag.doc_md = __doc__

    def insert_new_record(**kwargs):
        scdtype2.process()
        return {"msg": "ok"}

    # [START check_data]
    def update_data_changed():
        scdtype2.update()
        return {"status ": "Success !"}
    # [END check_data]

    insert_task = PythonOperator(
        task_id='insert_task',
        python_callable=insert_new_record,
    )
    insert_task.doc_md = dedent(
        """\
    #### Insert task
    Insert new records
    """
    )

    update_data_changed_task = PythonOperator(
        task_id='update_data_changed_task',
        python_callable=update_data_changed,
    )
    update_data_changed_task.doc_md = dedent(
        """\
    #### Update task
    Update record has changed
    """
    )

    insert_task >> update_data_changed_task

# [END main_flow]

# [END tutorial]
