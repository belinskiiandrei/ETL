import pandas as pd
import datetime as dt
import pandahouse as ph
from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'owner': 'a.belinskii',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'start_date': datetime(2026, 2, 28),
}

schedule_interval = '0 23 * * *'

@dag(default_args = default_args, schedule_interval=schedule_interval, catchup=False)
def dag_daily_belinskii():

    @task()
    def extract_data_actions():
        connection = {
            'host': '******',
            'database': '******',
            'user': '******',
            'password': '******'
        }

        q = """
        SELECT 
            user_id,
            sum(action='like') as likes,
            sum(action='view') as views,
            os,
            age,
            gender
        FROM
            {db}.feed_actions
        WHERE
            toDate(time) = today() - 1
        GROUP BY
            user_id, os, age, gender
        """

        actions = ph.read_clickhouse(q, connection=connection)
        return actions

    @task()
    def extract_data_messages():
        connection = {
            'host': '******',
            'database': '******',
            'user': '******',
            'password': '******'
        }

        q = """
        WITH cte AS (
        SELECT
            receiver_id,
            count() as total_accept,
            uniqExact(user_id) as uniq_sender
        FROM
            simulator_20260120.message_actions
        WHERE 
            toDate(time) = today() - 1    
        GROUP BY
            receiver_id
        )    
    
        SELECT 
            user_id,
            total_accept,
            count() as total_send_message,
            uniqExact(receiver_id) as uniq_recipient,
            uniq_sender
        FROM 
            simulator_20260120.message_actions ma
        JOIN cte
            on cte.receiver_id = ma.user_id
        WHERE 
            toDate(time) = today() - 1
        GROUP BY 
            user_id, total_accept, uniq_sender, gender, os, age
        """

        message_data = ph.read_clickhouse(q, connection=connection)

        return message_data

    @task()
    def merge_datasets(feed_data, data_mesage):
        result = feed_data.merge(data_mesage, how='left', on='user_id', suffixes=['_feed', '_message'])
        result = result.fillna(0)

        result['total_accept'] = result['total_accept'].astype('int64')
        result['total_send_message'] = result['total_send_message'].astype('int64')
        result['uniq_recipient'] = result['uniq_recipient'].astype('int64')
        result['uniq_sender'] = result['uniq_sender'].astype('int64')

        return result

    @task()
    def cut_os(merged_data, cut='os'):
        result = merged_data.groupby(cut, as_index=False).agg({'views': 'sum',
                                                               'likes': 'sum',
                                                               'total_accept': 'sum',
                                                               'total_send_message': 'sum',
                                                               'uniq_recipient': 'sum',
                                                               'uniq_sender': 'sum'})
        result['event_date'] = dt.date.today() - dt.timedelta(days=1)
        result['dimension'] = cut

        result = result.rename(columns={'total_accept': 'messages_received',
                                        'total_send_message': 'messages_sent',
                                        'uniq_sender': 'users_sent',
                                        'uniq_recipient': 'users_received',
                                        'os': 'dimension_value'})

        columns_order = ['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received',
                         'messages_sent', 'users_received', 'users_sent']

        result = result.reindex(columns=columns_order)

        return result

    @task()
    def cut_age(merged_data, cut='age'):
        result = merged_data.groupby(cut, as_index=False).agg({'views': 'sum',
                                                               'likes': 'sum',
                                                               'total_accept': 'sum',
                                                               'total_send_message': 'sum',
                                                               'uniq_recipient': 'sum',
                                                               'uniq_sender': 'sum'})
        result['event_date'] = dt.date.today() - dt.timedelta(days=1)
        result['dimension'] = cut

        result = result.rename(columns={'total_accept': 'messages_received',
                                        'total_send_message': 'messages_sent',
                                        'uniq_sender': 'users_sent',
                                        'uniq_recipient': 'users_received',
                                        'age': 'dimension_value'})

        columns_order = ['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received',
                         'messages_sent', 'users_received', 'users_sent']

        result = result.reindex(columns=columns_order)

        return result

    @task()
    def cut_gender(merged_data, cut='gender'):
        result = merged_data.groupby(cut, as_index=False).agg({'views': 'sum',
                                                               'likes': 'sum',
                                                               'total_accept': 'sum',
                                                               'total_send_message': 'sum',
                                                               'uniq_recipient': 'sum',
                                                               'uniq_sender': 'sum'})
        result['event_date'] = dt.date.today() - dt.timedelta(days=1)
        result['dimension'] = cut

        result = result.rename(columns={'total_accept': 'messages_received',
                                        'total_send_message': 'messages_sent',
                                        'uniq_sender': 'users_sent',
                                        'uniq_recipient': 'users_received',
                                        'gender': 'dimension_value'})

        columns_order = ['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received',
                         'messages_sent', 'users_received', 'users_sent']

        result = result.reindex(columns=columns_order)

        return result

    @task()
    def do_result_df(cut_gender, cut_age, cut_os):
        connection = {
            'host': '******',
            'database': '******',
            'user': '******',
            'password': '******'
        }

        result = pd.concat([cut_gender, cut_age, cut_os])

        ph.to_clickhouse(
            df=result,
            table='yesterday_metrics_for_report',
            index=False,
            connection=connection
        )

    #Первый этап - выгрузка данных
    data_actions = extract_data_actions()
    date_messages = extract_data_messages()

    #Второй этап - объединение данных в один датасет
    merged_data = merge_datasets(data_actions, date_messages)

    #Третий этап - посдчет метрик в разрезе
    os_cut = cut_os(merged_data)
    age_cut = cut_age(merged_data)
    gender_cut = cut_gender(merged_data)

    #Четвертый этап - загрузка данных в таблицу
    do_result_df(gender_cut, age_cut, os_cut)

dag_daily_belinskii = dag_daily_belinskii()