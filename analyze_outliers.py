import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

db_path = "companies.db"
try:
    cnx = sqlite3.connect(db_path)
    print("База данных подключена успешно")

    df = pd.read_sql_query("""
        SELECT * FROM companies
        WHERE revenue IS NOT NULL 
        AND growth_rate IS NOT NULL AND revenue<15000000000
    """, cnx)
    print(f"Загружено {len(df)} записей")

    # Очистка данных от выбросов методом z-score
    def remove_outliers(group):
        z_scores = stats.zscore(group[['revenue', 'growth_rate']], nan_policy='omit')
        return group[(abs(z_scores) < 3).all(axis=1)]

    # Очищаем данные по каждому ОКВЭД отдельно
    df_cleaned = df.groupby('okved_1').apply(remove_outliers).reset_index(drop=True)
    print(f"После удаления выбросов осталось {len(df_cleaned)} записей")

    # Подсчитываем количество компаний в каждом ОКВЭД
    okved_counts = df_cleaned['okved_1'].value_counts()
    
    # Фильтруем ОКВЭД с малой выборкой
    min_companies = 0
    valid_okveds = okved_counts[okved_counts >= min_companies].index

    df_cleaned = df_cleaned[df_cleaned['okved_1'].isin(valid_okveds)]
    print(f"После удаления ОКВЭД с малой выборкой осталось {len(df_cleaned)} записей")
    print("\nКоличество компаний по ОКВЭД после фильтрации:")
    print(df_cleaned['okved_1'].value_counts())

    okved_analysis = pd.DataFrame()

    # 1. Базовая статистика
    basic_stats = df_cleaned.groupby('okved_1').agg({
        'revenue': ['count', 'mean', 'std', 'median'],
        'growth_rate': ['mean', 'std', 'median']
    }).round(2)

    # 2. Расчет дополнительных метрик
    for okved in df_cleaned['okved_1'].unique():
        okved_data = df_cleaned[df_cleaned['okved_1'] == okved]
        
        analysis = {
            'okved': okved,
            'company_count': len(okved_data),
            'avg_revenue': okved_data['revenue'].mean(),
            'avg_growth': okved_data['growth_rate'].mean(),
            'revenue_std': okved_data['revenue'].std(),
            'growth_std': okved_data['growth_rate'].std(),

            'revenue_cv': okved_data['revenue'].std() / okved_data['revenue'].mean() * 100,
            'growth_cv': okved_data['growth_rate'].std() / okved_data['growth_rate'].mean() * 100,

            'revenue_median': okved_data['revenue'].median(),
            'growth_median': okved_data['growth_rate'].median(),

            'revenue_q1': okved_data['revenue'].quantile(0.25),
            'revenue_q3': okved_data['revenue'].quantile(0.75),
            'growth_q1': okved_data['growth_rate'].quantile(0.25),
            'growth_q3': okved_data['growth_rate'].quantile(0.75),

            'revenue_iqr': okved_data['revenue'].quantile(0.75) - okved_data['revenue'].quantile(0.25),
            'growth_iqr': okved_data['growth_rate'].quantile(0.75) - okved_data['growth_rate'].quantile(0.25),

            'total_revenue': okved_data['revenue'].sum(),

            'positive_growth_ratio': (okved_data['growth_rate'] > 0).mean() * 100,

            'perspective_score': (
                okved_data['growth_rate'].mean() * 0.4 +  # Средний рост
                (okved_data['revenue'].mean() / df_cleaned['revenue'].mean()) * 0.3 +  # Относительная выручка
                ((okved_data['growth_rate'] > 0).mean() * 100) * 0.3  # Доля растущих компаний
            )
        }
        
        okved_analysis = pd.concat([okved_analysis, pd.DataFrame([analysis])], ignore_index=True)

    okved_analysis = okved_analysis.sort_values('perspective_score', ascending=False)


    with pd.ExcelWriter('industry_analysis_cleaned.xlsx') as writer:

        okved_analysis.to_excel(writer, sheet_name='General_Analysis', index=False)

        for okved in df_cleaned['okved_1'].unique():
            top_companies = df_cleaned[df_cleaned['okved_1'] == okved].nlargest(10, 'revenue')[
                ['name', 'inn', 'revenue', 'growth_rate', 'okved']
            ]
            top_companies.to_excel(writer, sheet_name=f'Top10_{okved}', index=False)


    print("\nТоп-5 перспективных отраслей:")
    print(okved_analysis[['okved', 'perspective_score', 'avg_revenue', 'avg_growth', 'company_count']].head())

    plt.figure(figsize=(15, 10))

    # График распределения выручки по отраслям
    plt.subplot(2, 1, 1)
    sns.boxplot(data=df_cleaned, x='okved_1', y='revenue')
    plt.title('Распределение выручки по отраслям')
    plt.xticks(rotation=45)

    # График распределения роста по отраслям
    plt.subplot(2, 1, 2)
    sns.boxplot(data=df_cleaned, x='okved_1', y='growth_rate')
    plt.title('Распределение темпов роста по отраслям')
    plt.ylim(-100, 300)
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.savefig('industry_distribution.png')

except Exception as e:
    print(f"Произошла ошибка: {e}")
finally:
    if 'cnx' in locals():
        cnx.close() 