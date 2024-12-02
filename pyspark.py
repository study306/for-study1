import streamlit as st

# Function to display code examples
def display_code(code: str):
    st.code(code, language='python')

# Code examples
rdd_operations_example_1 = '''
from pyspark.sql import SparkSession

spark = SparkSession.builder\.appName("RDD Operations Example")\.getOrCreate()

csv_rdd = spark.sparkContext.textFile("iris.csv")

csv_rdd = csv_rdd.map(lambda line: line.split(","))

rdd_mapped = csv_rdd.map(lambda x: [float(i) * 2 if i.replace('.', '', 1).isdigit() else i for i in x])

print("Mapped RDD:")
print(rdd_mapped.collect())

print("Row count:")
print(csv_rdd.count())

print("First 3 rows:")
print(csv_rdd.take(3))
'''

rdd_operations_example_2 = '''
from pyspark import SparkContext

sc = SparkContext("local", "RDD Operations Example")

data = [
    "Spark is fast",
    "Spark is powerful",
    "RDD is resilient",
    "Hadoop is old",
    "Spark has a powerful ecosystem"
]

rdd = sc.parallelize(data)

# 1. Map Operation
rdd_mapped = rdd.map(lambda x: x.split(" "))
print("Mapped (split into words):")
print(rdd_mapped.collect())

# 2. Filter Operation
rdd_filtered = rdd.filter(lambda x: "Spark" in x)
print("\\nFiltered (containing 'Spark'):")
print(rdd_filtered.collect())

# 3. FlatMap Operation
rdd_flatmapped = rdd.flatMap(lambda x: x.split(" "))
print("\\nFlatMapped (flattened words):")
print(rdd_flatmapped.collect())

# 4. Distinct Operation
rdd_distinct = rdd_flatmapped.distinct()
print("\\nDistinct (unique words):")
print(rdd_distinct.collect())

# 5. Word Count
words = rdd.flatMap(lambda x: x.split(" "))
word_pairs = words.map(lambda word: (word, 1))
word_count = word_pairs.reduceByKey(lambda a, b: a + b)
print("\\nWord Count:")
print(word_count.collect())

sc.stop()
'''

sampling_filtering_example_1 = '''
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(data)

sampled_rdd = rdd.sample(withReplacement=False, fraction=0.4, seed=42)

print("Sampled RDD:", sampled_rdd.collect())

filtered_rdd = rdd.filter(lambda x: x % 2 == 0)
print("Filtered RDD:", filtered_rdd.collect())
'''

sampling_filtering_example_2 = '''
from pyspark import SparkContext

sc = SparkContext("local", "Sampling and Filtering Example")

data = [
    "Spark is fast",
    "Spark is powerful",
    "RDD is resilient",
    "Hadoop is old",
    "Spark has a powerful ecosystem",
    "Machine Learning with Spark",
    "Big Data and Hadoop",
    "Data Analytics with Spark"
]

rdd = sc.parallelize(data)

# 1. Filter Operation
spark_related = rdd.filter(lambda x: "Spark" in x)
print("Filtered (containing 'Spark'):")
print(spark_related.collect())

# 2. Sample Operation
sampled_data = rdd.sample(withReplacement=False, fraction=0.4, seed=42)
print("\\nSampled Data (40% of total):")
print(sampled_data.collect())

# 3. Combine Filtering and Sampling
hadoop_related = rdd.filter(lambda x: "Hadoop" in x)
sampled_hadoop = hadoop_related.sample(withReplacement=False, fraction=0.5, seed=42)
print("\\nSampled Hadoop-Related Data (50% of filtered):")
print(sampled_hadoop.collect())

sc.stop()
'''

sampling_filtering_example_3 = '''
from pyspark import SparkContext

sc = SparkContext.getOrCreate()  

data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = sc.parallelize(data)

even_rdd = rdd.filter(lambda x: x % 2 == 0)  
odd_rdd = rdd.filter(lambda x: x % 2 != 0)   

combined_rdd = even_rdd.union(odd_rdd)  

squared_rdd = combined_rdd.map(lambda x: x ** 2)

print("Original RDD:", rdd.collect())
print("Even RDD:", even_rdd.collect())
print("Odd RDD:", odd_rdd.collect())
print("Combined RDD:", combined_rdd.collect())
print("Squared Combined RDD:", squared_rdd.collect())

sc.stop()
'''

spark_plotting_example = '''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import matplotlib.pyplot as plt

schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Year", IntegerType(), True)
])

spark = SparkSession.builder.appName("PyWithPython").getOrCreate()
sc = spark.sparkContext

data = [
    ("12345", "ProductA", 10, 2020),
    ("12346", "ProductB", 5, 2020),
    ("12347", "ProductA", 15, 2021),
    ("12348", "ProductC", 20, 2021),
    ("12349", "ProductB", 25, 2022),
    ("12350", "ProductA", 30, 2022),
    ("12351", "ProductC", 35, 2023),
    ("12352", "ProductB", 40, 2023)
]

rdd = spark.sparkContext.parallelize(data)

df = spark.createDataFrame(rdd, schema)

df_grouped = df.groupBy("Year").sum("Quantity")

df_pandas = df_grouped.toPandas()

x = df_pandas["Year"].values.tolist()
y = df_pandas["sum(Quantity)"].values.tolist()

plt.plot(x, y, marker='o', color='blue', label="Sales Trend")
plt.title("Sales Trend Over Years")
plt.xlabel("Year")
plt.ylabel("Quantity")
plt.legend()
plt.grid(True)
plt.show()

plt.bar(x, y, color="green", label="Sales Visualization")
plt.title("Sales Report")
plt.xlabel("Year")
plt.ylabel("Quantity")
plt.legend()
plt.show()

spark.stop()
'''

# Streamlit App Layout
st.title("PySpark Experiments")

# Sidebar for selecting experiments
selected_experiment = st.sidebar.selectbox(
    "Select an Experiment", 
    ["RDD Operations Example 1", "RDD Operations Example 2", 
     "Sampling and Filtering Example 1", "Sampling and Filtering Example 2", 
     "Sampling and Filtering Example 3", "Spark Plotting Example"]
)

# Display corresponding code
if selected_experiment == "RDD Operations Example 1":
    st.header("RDD Operations Example 1")
    display_code(rdd_operations_example_1)
elif selected_experiment == "RDD Operations Example 2":
    st.header("RDD Operations Example 2")
    display_code(rdd_operations_example_2)
elif selected_experiment == "Sampling and Filtering Example 1":
    st.header("Sampling and Filtering Example 1")
    display_code(sampling_filtering_example_1)
elif selected_experiment == "Sampling and Filtering Example 2":
    st.header("Sampling and Filtering Example 2")
    display_code(sampling_filtering_example_2)
elif selected_experiment == "Sampling and Filtering Example 3":
    st.header("Sampling and Filtering Example 3")
    display_code(sampling_filtering_example_3)
elif selected_experiment == "Spark Plotting Example":
    st.header("Spark Plotting Example")
    display_code(spark_plotting_example)
