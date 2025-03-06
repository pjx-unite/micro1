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
