import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Récupération des arguments Glue
args = getResolvedOptions(sys.argv, ['output_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

output_path = args['output_path']

# Générer les données 1 → 20
data = [(i,) for i in range(1, 21)]

df = spark.createDataFrame(data, ["number"])

df.show()

# Write vers S3 (path déjà fourni par Terraform)
df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"Data written to: {output_path}")
