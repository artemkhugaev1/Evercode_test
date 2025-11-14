# Evercode_test
Here is my solution for test tasks

Выполненное задание по SQL лежат в sql_task
sql_task:

  task_1_data -- Решение первого задания
  
    metrics_partner_id.csv -- метрики в разрезе по партнерам
    
    metrics_network_id.csv -- метрики в разрезе по сетям
    
    metrics_market_share.csv -- метрика показывающая нашу долю за день от всего рынка по currency_id
    
  task_2_data -- Решение второго задания
  
    task2_H1.csv -- данные для проверки первой гипотезы
    
    task2_H2(1).csv -- данные для проверки второй гипотезы (ticker_id = 2)
    
    task2_H2(2).csv -- данные для проверки второй гипотезы (ticker_id = 86)
    
  task_3_data -- Решение третьего задания
  
    task3_(partners).csv -- список топ-20 партнеров отсортированных по объему
    
    task3_(ticker_id).csv -- список топ-20 активов с высоким объемом и avg_volume_per_exchange -  median_volume  < 95 percentile
    
    task3_(network_id).csv -- список сетей с avg_growht < 0.05 percentile
    
  task_4_data -- Решение четвертого задания
  
    task_4(key_metric).csv -- таблица содержит stability_index (1-(avg_duration_seconds - median_duration_seconds)/avg_duration_seconds
) по дням

python_task:

  API_work.ipynb -- полный процесс работы по сбору и проверке монет
  
  API_task_result.csv -- итоговая таблица для с монетами  из эндпоинта coinmarketcap, которых
                          нет в списке доступных у обменника https://simpleswap.io добавления в базу данных
