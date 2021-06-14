from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 table,
                 variables,
                 insert_query,
                 create_stmt,
                 drop_stmt,
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.variables = variables
        self.insert_query = insert_query
        self.create_stmt = create_stmt
        self.truncate = truncate
        self.drop_stmt = drop_stmt
        

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        # Drop Table
        redshift.run(self.drop_stmt)
        
        # Create Table
        redshift.run(self.create_stmt)
        
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
            
        redshift.run("INSERT INTO {} {} {}".format(self.table, self.variables, self.insert_query))

        
