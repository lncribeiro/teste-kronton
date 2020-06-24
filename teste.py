from pyspark.sql import functions as f
from pyspark.sql import SparkSession


class Transformation:

    def __init__(self, id_execution, path_to_read, path_to_save):
        self.id_execution = id_execution
        self.path_to_read = path_to_read
        self.spark = SparkSession.builder.appName('teste').getOrCreate()
        self.path_to_save = path_to_save

    def transform(self):

        df = self.read()
        df = self.change_columns(df)
        df = self.drop_columns(df)
        df = self.select_columns(df)
        self.save_to_s3(df)

    def read(self):
        return self.spark.read.load(self.path_to_read)

    def change_columns(self, df):
        df = df.withColumn("ID_EXECUCAO", f.lit(self.id_execution))
        df = df.withColumn("CODIGO_ALUNO", f.col("CDALUN"))

        # caso COD1 não seja igual a "A", a coluna "DECISAO" deve ser False
        df = df.withColumn("DECISAO", f.when(f.lit("COD1") == "A", f.lit(True)).otherwise(f.lit(False)))

        return df

    def drop_columns(self, df):
        df = df.drop("NOT_USED_COLUMN")
        return df

    def select_columns(self, df):
        # selecionar todas as colunas presentes no dataFrame
    	return df.select('*').collect()

    def save_to_s3(self, df):
    	# salvar no s3 em formato parquet no 'path_to_save'
        df.to_parquet(self.path_to_save)