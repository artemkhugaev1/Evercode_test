-- Задача 1.

-- Ключевые показатели, которые должны быть отражены в Дашборде

	-- Количество сделок
		--Метрика: amount_of_exchange
		--Показывает: количество операций за день
		--Почему важна: помогает отслеживать рост\падения количества операций
		--Как считается:  count(*) as amount_of_exchange
	

	-- Метрики на основе Amount_in_base_currency сумма депозита в универсальной валюте:
		--Метрика: our_volume_sum
		--Показывает: объем обменов за день
		--Почему важна: помогает отслеживать рост\падение объем обменов за день
		--Как считается: sum(amount_in_base_currency) as our_volume_sum
	
		--Метрика: median_volume
		--Показывает: медианный объем депозитов, которые прохродят через обменник. 
		--Почему важна: предполагается, что amount_in_base_currency высоко валатильна, счиатем именно медиану потому, она более устойчива к выбросам чем среднее.
		--Как считается: percentile_cont(0.5) within group (order by amount_in_base_currency) as median_volume
	
		--Метрика: avg_volume_per_exchange
		--Показывает: средний объём одной сделки (средний чек одной транзакции).
		--Почему важна: помогает понять каков типичный размер операции у партнёров или в конкретной сети + отслеживать динамику.
		--Как считается: sum(amount_in_base_currency)::numeric / count(*)
	
		--Метрика: volume_growth_pct
		--Показывает: динамику роста/падения объёма обменов по отношению к предидущему дню (в %). 
		--Почему важна: дает сигналы резкий рост --> партнёр привёл новых пользователей / акция / маркетинг; резкое падение → сбой, потеря клиентов, технические проблемы.
		--Как считается: round(
	   --((our_volume_sum - lag(our_volume_sum) over (partition by partner_id order by date1))/ 
	   --lag(our_volume_sum) over (partition by partner_id order by date1))::numeric  * 100,
	   --2) as volume_growth_pct

   -- Метрики на основе exchange_duration - время работы с заявкой:
	   --Метрики: avg_duration_seconds, median_duration_seconds
	   --Показывают: среднее и медианное время совершенния сделки
	   --Почему важна: влияет на retention пользователя.
	   --Считаем среднее, так как аномально высокие значения могут означать проблемы в сети или у партнера. Важно видеть их, чтоб устранить
	   --Считаем медиану, так как важно видеть более стабильную картину того, как сеть работает быстрее или у какого партнера обработака заявок происходит быстрее
	   --Как считается:  round(avg(extract(epoch from exchange_duration ::interval)),2) as avg_duration_seconds, 
		--	percentile_cont(0.5) within group (order by extract(epoch from exchange_duration ::interval)) as median_duration_seconds
		

-- ТАБЛИЦА 1. Считаем эти метрики в разрезе по partner_id. Получим metrics_partner_id.csv
with daily_stats as (
    select 
        date(created_at) as date1, 
        partner_id, 
        round(avg(extract(epoch from exchange_duration ::interval)),2) as avg_duration_seconds, 
		percentile_cont(0.5) within group (order by extract(epoch from exchange_duration ::interval)) as median_duration_seconds,
        count(*) as amount_of_exchange,
        sum(amount_in_base_currency) as our_volume_sum, 
        percentile_cont(0.5) within group (order by amount_in_base_currency) as median_volume,
        sum(amount_in_base_currency)::numeric / count(*) as avg_volume_per_exchange
    from exchange e
    group by date1, partner_id
)
select 
    date1,
    partner_id, avg_duration_seconds, median_duration_seconds,
    amount_of_exchange,
    our_volume_sum,
    median_volume,
    avg_volume_per_exchange,

    -- объем за предыдущий день
   lag(our_volume_sum) over (partition by partner_id order by date1) as prev_valume,
   
   round(
   ((our_volume_sum - lag(our_volume_sum) over (partition by partner_id order by date1))/ 
   lag(our_volume_sum) over (partition by partner_id order by date1))::numeric  * 100,
   2) as volume_growth_pct

from daily_stats
order by date1, partner_id ;

-- ТАБЛИЦА 2. Считаем эти метрики в разрезе по network_id. Получим metrics_network_id.csv
with daily_stats1 as (
    select date(created_at) as date1, 
        network_id , 
        round(avg(extract(epoch from exchange_duration ::interval)),2) as avg_duration_seconds, 
		percentile_cont(0.5) within group (order by extract(epoch from exchange_duration ::interval)) as median_duration_seconds,
        count(*) as amount_of_exchange,
        sum(amount_in_base_currency) as our_volume_sum, 
        percentile_cont(0.5) within group (order by amount_in_base_currency) as median_volume,
        sum(amount_in_base_currency)::numeric / count(*) as avg_volume_per_exchange
from exchange e 
inner join ticker t on t.ticker_id = e.ticker_id 
group by date1, network_id 
order by date1, network_id
)
select 
     date1,
    network_id, avg_duration_seconds, median_duration_seconds,
    amount_of_exchange,
    our_volume_sum,
    median_volume,
    avg_volume_per_exchange,
   lag(our_volume_sum) over (partition by network_id order by date1) as prev_valume,
   
   round(
   ((our_volume_sum - lag(our_volume_sum) over (partition by network_id order by date1))/ 
   lag(our_volume_sum) over (partition by network_id order by date1))::numeric  * 100,
   2) as volume_growth_pct

from daily_stats1
order by date1, network_id;



	-- Метрика на основе volume24h рыночный оборот за 24ч:
		--Метрика: market_share
		--Показывает: нашу долю объем обменов от общего рыночного оборота за 24ч по конкретной currency.
		--Почему важна: позволяет оценить рост компании относительно рынка
		--Как считается: (dv.our_24h_volume / m.volume24h) *100  as market_share

-- ТАБЛИЦА 3. Считаем эти метрики в разрезе по currency_id. Получим metrics_market_share.csv
	
select dv.date1, dv.currency_id, dv.our_24h_volume,  m.volume24h,
(dv.our_24h_volume / m.volume24h) *100  as market_share

from(with daily_volume as (select date(created_at) as date1, currency_id, sum(amount_in_base_currency) as our_24h_volume,
date(created_at)::text || '_' || currency_id::text as join_key
from exchange e 
inner join ticker t on t.ticker_id = e.ticker_id 
group by  date1, currency_id
having sum(amount_in_base_currency) is not null
order by date1, currency_id) 


select * from daily_volume
) dv
inner join mvolume m on dv.join_key = m."date" || '_' || m.currency_id::text;


-- Задача 2
С чем связана динамика оборота? Предложите несколько гипотез. Порассуждайте и
проверьте их. Если данных недостаточно, напишите алгоритм, что вам нужно и что вы
будете делать
-- H1: Динамика оборота зависит от дня недели: в будние дни динамика выше, так как совершается больше операций
-- Результат task2_H1.csv
select 
    to_char(created_at::timestamp, 'Dy') as weekday,
    extract(dow from created_at::timestamp) as day_of_week,
    sum(amount_in_base_currency) as our_volume_sum,
    avg(amount_in_base_currency) as avg_exchange_volume,
    count(*) as exchange_count
from exchange e
inner join ticker t on e.ticker_id = t.ticker_id
group by weekday, day_of_week
order by day_of_week;

--Вывод: очевидных закономерностей нет, но видим, что максимальный объем сделок достигается в ПН, ВТ и ЧТ. Однако, количество сделок очевидно держится высоким в ВС, ПН, ВТ
--а затем заметно уменьшается.
--Что еще посмотреть: 1) Данные за более длительный промежуток у нас всего с 19.12 по 04.01 (этого может быть мало для стабильной оценки)
--					  2) Данные по работам сетей, возможно в какой-то момент была просадка в определенных сетях, что привело к снижению оборота

--H2: Динамика оборота зависит от актива: активы с большими объемами менее стабильны
-- Посмотрим на актив (ticker_id) с самым большим объемом

select ticker_id, sum(amount_in_base_currency) as our_volume_sum
from exchange e 
group by ticker_id 
order by our_volume_sum desc
limit 1;
-- ticker id = 2

-- посмотрим как объем изменяется в процентах от дня ко дню (метрика volume_growth_pct)
-- Результат task2_H2(1).csv
with daily_stats1 as (
    select date(created_at) as date1, 
        ticker_id , 
        sum(amount_in_base_currency) as our_volume_sum 
from exchange e 
group by date1, ticker_id 
order by date1, ticker_id 
)
select 
     date1,
    ticker_id, 
    our_volume_sum,   
   round(
   ((our_volume_sum - lag(our_volume_sum) over (partition by ticker_id order by date1))/ 
   lag(our_volume_sum) over (partition by ticker_id order by date1))::numeric  * 100,
   2) as volume_growth_pct

from daily_stats1
where ticker_id = 2
order by date1;

--Вывод: действительно видим, что объем актива может как резко рости (+308%), так и резко падать (-85%). Видим некоторую нестабильность

-- Для сравнения посмотрим на какой-нибудь средний по объему актив. Пусть это будет ticker_id 86 с объемом 101.110466

-- Результат task2_H2(2).csv
with daily_stats1 as (
    select date(created_at) as date1, 
        ticker_id , 
        sum(amount_in_base_currency) as our_volume_sum 
from exchange e 
group by date1, ticker_id 
order by date1, ticker_id 
)
select 
     date1,
    ticker_id, 
    our_volume_sum,   
   round(
   ((our_volume_sum - lag(our_volume_sum) over (partition by ticker_id order by date1))/ 
   lag(our_volume_sum) over (partition by ticker_id order by date1))::numeric  * 100,
   2) as volume_growth_pct

from daily_stats1
where ticker_id = 86
order by date1;

--Вывод: однако, видим тут еще более сильную волатильность рост на 1998%, так и падения на 80%
-- Однако для достоверного вывода стоит взять большую выборку, сегментировать активы по размерам и провести стат.тесты, 
--чтобы узнать, есть ли стат.значимая разница

--H3: Динамика оборота зависит от внешних рыночных новостей или событий
-- После новостей (изменения курса, выход экономических данных, санкции) пользователи активнее торгуют.
-- Для проверки нужно: взять топ источников новостей и собрать данные. Затем (вручную или с помощью LLM) разметить новости 
-- (есть/нет значимые события, которые могут повлиять на рынок), 
--затем сравнить динамику оборота в зависимости от того были ли значимые новости для рынка


--Задача 3

-- Персональных менеджеров будем прикреплять к партнерам которые:
	--a) в топ-10%  по обороту (Метрика: our_volume_sum) всего 236 партнеров, возьмем топ-20
	--b) высокая клиентская активность (Метрика: volume_growth_pct)
-- Это делается, так как партнеры наиболее крупные и можно развивать сотрудничество и поддержку продукта для них, чтобы привлекать через этот канал еще больше пользователей
-- Особенно интересны такие как partner_id = 101, где при небольшом количестве сделок 270, высокий объем обменов ~ 12k, 
-- рост пользователей через такие каналы, принесет максимальный рост объма

-- Результат: task_3(partners).csv
select 
   partner_id, 
   count(*) as amount_of_exchange,
   sum(amount_in_base_currency) as our_volume_sum 
   from exchange e
   group by partner_id
   having sum(amount_in_base_currency) is not null
   order by our_volume_sum desc
   limit 20


-- Концентрироваться будем на активах, которые имеют:
	-- a) высокий суммарный объём (our_volume_sum),
	-- b) умеренная волатильность (median_volume - avg_volume_per_exchange < 95percentile).
   
-- Считаем 95-перцентиль, чтобы отсеять активы, которые имеют сильные выбросы
 
select percentile_cont(0.95) within group (order by diff)
from(
select ticker_id, sum(amount_in_base_currency)::numeric / count(*) - percentile_cont(0.5) within group (order by amount_in_base_currency) as diff
    from exchange e
    group by ticker_id  
); -- Получили ~9

-- Отбираем топ 20 активов из 187 с наибольшим объемом и наиболее стабильные

-- Результат: task_3(ticker_id).csv
with daily_stats as (
    select 
        ticker_id, 
        sum(amount_in_base_currency) as our_volume_sum, 
        percentile_cont(0.5) within group (order by amount_in_base_currency) as median_volume,
        sum(amount_in_base_currency)::numeric / count(*) as avg_volume_per_exchange,
        sum(amount_in_base_currency)::numeric / count(*) - percentile_cont(0.5) within group (order by amount_in_base_currency) as diff
    from exchange e
    group by ticker_id
)
select 
    ticker_id, our_volume_sum, diff
from daily_stats
where diff < 9
order by our_volume_sum desc 
limit 20;

Какие сети проседают?
-- рост сетей по метрике volume_growth_pct (рост объема в % от предидущего дня)
-- мы сначала посчитаем avg_growht - это средний рост по сетям, затем сравним его с 0.05 перцентилем, то есть отсеем 5% самых медленно растущих по общему объему сетей

-- Результат: task_3(network_id).csv
	with network_avg as ( select network_id, avg(volume_growth_pct) as avg_growht
	from (
	with daily_stats1 as (
	select date(created_at) as date1, network_id, sum(amount_in_base_currency) as our_volume_sum 
	from exchange e 
	inner join ticker t on t.ticker_id = e.ticker_id 
	group by date1, network_id
	)
	select date1, network_id,    
	              ((our_volume_sum - lag(our_volume_sum) over (partition by network_id order by date1))::numeric
	               / lag(our_volume_sum) over (partition by network_id order by date1) * 100) as volume_growth_pct
	        from daily_stats1
	) t
	group by network_id
),
	percentile_95 as (
	    select percentile_cont(0.05) within group (order by avg_growht) as p95
	    from network_avg
)
select *
from network_avg
where avg_growht <= (select p95 from percentile_95)
order by avg_growht desc;

--Задача 4
	-- Метрика: stability_index

-- Показывает: стабильность скорости обработки заявок.

-- Если индекс близок к 1 --> средняя и медианная длительность схожи → сервис работает стабильно.

-- Если индекс низкий --> есть скачки, отдельные долгие заявки --> возможные проблемы.

	--Почему это ключевая метрика для оценки сервиса: 
--если  время сделки растёт — пользователи чувствуют замедление и неудобство. Предположительно, может снизиться retetntion. 
--Если снижается — повышается скорость обработки и качество сервиса. Так же эта метрика более объективна, так как в данных есть сильные выбросы, которые нужно учитывать, 
-- так как видимо криптообменики часто работают нестабильно

-- Как считается: 1 - (avg_duration_seconds - median_duration_seconds)/avg_duration_seconds
	
--Рассчет по дням:

-- Результат: task_4(key_metric).csv
select date(created_at) as date1,  
    round(
        1 - (
            (avg(extract(epoch from exchange_duration::interval))::numeric - 
             percentile_cont(0.5) within group (order by extract(epoch from exchange_duration::interval))::numeric)
            / nullif(avg(extract(epoch from exchange_duration::interval))::numeric, 0)
        ), 
    2) as stability_index
from exchange e 
group by date1
order by date1;