# micro1
import re
import pandas as pd

def extract_chinese(text):
    """Extract Chinese characters from text. Return None if none found."""
    chinese_chars = re.findall(r'[\u4e00-\u9fff]+', text)
    return ''.join(chinese_chars) if chinese_chars else None

def process_text_file(input_file, output_file):
    processed_data = []

    with open(input_file, 'r', encoding='utf-8') as file:
        for line in file:
            columns = line.strip().split('#')
            extracted = [extract_chinese(col) for col in columns]
            processed_data.append(extracted)

    # Convert to DataFrame and save to Excel
    df = pd.DataFrame(processed_data)
    df.to_excel(output_file, index=False, header=False)

# Usage Example
process_text_file('input.txt', 'output.xlsx')


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
import re
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder.appName("ExtractChineseToExcel").getOrCreate()

def extract_chinese(text):
    """Extract Chinese characters from text. Return None if none found."""
    chinese_chars = re.findall(r'[\u4e00-\u9fff]+', text)
    return ''.join(chinese_chars) if chinese_chars else None

def process_text_file(input_file, output_file):
    # Read file into an RDD
    rdd = spark.read.text(input_file).rdd.map(lambda x: x[0])

    # Process each line: split by '#', extract Chinese characters, replace empty with None
    processed_rdd = rdd.map(lambda line: [extract_chinese(col) for col in line.split('#')])

    # Convert to DataFrame
    max_columns = processed_rdd.map(len).max()  # Find max number of columns
    df = processed_rdd.map(lambda row: row + [None] * (max_columns - len(row))).toDF()

    # Collect data and write to Excel
    data = df.collect()
    pd_df = pd.DataFrame(data)
    pd_df.to_excel(output_file, index=False, header=False)

# Example Usage
process_text_file('input.txt', 'output.xlsx')

# Stop Spark session
spark.stop()


<br/>

Aaaaaaaaaa
from pyspark.sql import SparkSession
import re
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder.appName("ExtractChineseToExcel").getOrCreate()

def extract_chinese(text):
    """Extract Chinese characters from text. Return None if none found."""
    chinese_chars = re.findall(r'[\u4e00-\u9fff]+', text)
    return ''.join(chinese_chars) if chinese_chars else None

def process_text_file(input_file, output_file, line_delimiter="\002", column_delimiter="\001"):
    """
    Process a text file where all data is in one line, using specified line and column delimiters.
    Default Hive delimiters:
      - line_delimiter = "\002" (CTRL-B) to separate records
      - column_delimiter = "\001" (CTRL-A) to separate fields
    """
    # Read the entire file as a single string
    with open(input_file, 'r', encoding='utf-8') as file:
        content = file.read().strip()  # Read all data into one string

    # Split content into individual records (lines)
    lines = content.split(line_delimiter)  # Default: Hive record delimiter "\002"

    # Process each line: split columns, extract Chinese characters
    processed_data = [[extract_chinese(col) for col in line.split(column_delimiter)] for line in lines]

    # Find max columns to ensure all rows have the same length
    max_columns = max(len(row) for row in processed_data)

    # Normalize row lengths by padding with None
    processed_data = [row + [None] * (max_columns - len(row)) for row in processed_data]

    # Convert to Pandas DataFrame and write to Excel
    df = pd.DataFrame(processed_data)
    df.to_excel(output_file, index=False, header=False)

# Example Usage
process_text_file('input.txt', 'output.xlsx', line_delimiter="\002", column_delimiter="\001")

# Stop Spark session
spark.stop()




