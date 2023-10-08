from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pymongo import MongoClient
import requests

# Inicializar Spark
spark = SparkSession.builder.master("local").appName("JSONPlaceholder").getOrCreate()

# Obtener datos de JSONPlaceholder
response = requests.get('https://jsonplaceholder.typicode.com/posts')
data = response.json()

# Crear un DataFrame Spark a partir de los datos
df = spark.createDataFrame(data)

# Filtrar y transformar los datos 
df_filtered = df.limit(10).select("userId", "id", "title", "body")

# Conexion con MongoDB
client = MongoClient("mongodb://admin:password@localhost:27017/")
db = client["db_pyspark_test"]
collection = db["datos_prueba"]

# Conversion del DataFrame Spark a un formato MongoDB 
posts = df_filtered.toJSON().map(lambda x: eval(x)).collect()

# Insertar los datos en MongoDB
collection.insert_many(posts)

# Cerrar la conexión con MongoDB
client.close()

# Cerrar la sesión de Spark
spark.stop()