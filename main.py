import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Чтение файла CSV
df = pd.read_csv('data.csv', delimiter=';')

# Преобразование иерархических данных
df['код'] = df['код'].str.replace('.', '_')
df.fillna('', inplace=True)

# Вычисление сумм по терминальным вершинам для каждого года
years = df.columns[2:]
for year in years:
    df[year] = pd.to_numeric(df[year], errors='coerce')
    df[year] = np.where(df[year].isnull(), df.groupby('код')[year].transform('sum'), df[year])

# Фильтрация только терминальных вершин
terminal_nodes = df[df['проект'].str.startswith('Data')]

# Итоговый датафрейм с суммами для каждой нетерминальной вершины и годовыми значениями
result = terminal_nodes.groupby('код').agg({'проект': lambda x: tuple(set(x)), 'проект': 'first', **{year: 'sum' for year in years}}).reset_index()

# Подключение к PostgreSQL и сохранение данных в таблицу
engine = create_engine('postgresql://user:password@localhost/postgres_database')
result.to_sql('projects', engine, index=False, if_exists='replace')
