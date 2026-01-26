import sys
import pandas as pd

print ('Executing pipeline/pipeline.py')


print('arguments', sys.argv)


month = int(sys.argv[1])

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

print(f'hello pipeline, month={month}')