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
print(result)

# # Подключение к PostgreSQL и сохранение данных в таблицу
engine = create_engine('postgresql://user:password@localhost/postgres_database')
result.to_sql('projects', engine, index=False, if_exists='replace')


# Создание и изменение таблицы вручную:

# import psycopg2
# 
# conn = psycopg2.connect(database="postgres_database", user="user", password="password", host="localhost")
# cursor = conn.cursor()
# cursor.execute('CREATE TABLE pandas_data (code VARCHAR(10), project VARCHAR(50), "2022" DECIMAL(6), "2023" DECIMAL(6), "2024" DECIMAL(6), "2025" DECIMAL(6))')
# for index, data in result.iterrows():
#     cursor.execute('INSERT INTO pandas_data (code, project, "2022", "2023", "2024", "2025") VALUES (%s, %s, %s, %s, %s, %s)',
#                    (data['код'], data['проект'], data['2022'], data['2023'], data['2024'], data['2025']))
#     
# conn.commit()
# cur.close()
# conn.close()
