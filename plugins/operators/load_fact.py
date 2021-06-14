from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table,
                 variables,
                 insert_query,
                 create_stmt,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.conn_id = conn_id
        self.table = table
        self.variables = variables
        self.insert_query = insert_query
        self.create_stmt = create_stmt

        

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        redshift.run(self.create_stmt)
        redshift.run("INSERT INTO {} {} {}".format(self.table, self.variables, self.insert_query))
        
        
