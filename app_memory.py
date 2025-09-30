import os
from catboost import CatBoostClassifier
import pandas as pd
from typing import List
from fastapi import FastAPI, Query
from datetime import datetime
from sqlalchemy import create_engine
from schema import PostGet


#import time
#import psutil
#from functools import lru_cache


# логирование времени выполнения
#def log_execution_time(stage_name: str, start_time: float):
#    elapsed = time.time() - start_time
#    print(f"[TIMING] {stage_name}: {elapsed:.3f} секунд")
#    return elapsed

#def check_memory_usage():
#    process = psutil.Process(os.getpid())
#    mem_info = process.memory_info()
#    print(f"Используется RAM: {mem_info.rss / 1024 / 1024:.2f} MB")


app = FastAPI()

def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000

    #start_time = time.time()

    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()

    # Логирование времени загрузки
    #log_execution_time("batch_load_sql", start_time)

    return pd.concat(chunks, ignore_index=True)

def get_model_path(path: str) -> str:
    """
    Возвращает корректный путь к модели.
    Если в среде указана переменная IS_LMS=1, используется специальный путь.
    """
    if os.environ.get("IS_LMS") == "1":
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

def load_models():

    #start_time = time.time()  # Замер времени

    """
    Загружает и возвращает модель из файла
    """
    model_path = (get_model_path
                  ("C:/Users/natal/PycharmProjects/PythonProject/final_project_2/catboost_model.cbm")
                  #("/my/super/path/catboost_model.cbm")
                   )
    model = CatBoostClassifier()
    model.load_model(model_path)

    # Логирование времени загрузки
    #log_execution_time("load_models", start_time)

    return model

def load_features(user_id: int = None): #-> pd.DataFrame:   #, timestamp

    #start_time = time.time()  # Замер времени

    posts_query = "SELECT * FROM natalikuva_features_lesson_225 LIMIT 400000" # ORDER BY post_id DESC LIMIT 10000

    if user_id is not None:
        # Запросы для конкретного пользователя
        likes_query = f"""
               SELECT post_id, user_id 
               FROM feed_data 
               WHERE action='like' AND user_id = {user_id}
           """
        user_query = f"""
               SELECT * 
               FROM user_features_natalikuva_features_lesson_22
               WHERE user_id = {user_id}
           """
    else:
        # Полная загрузка
        likes_query = "SELECT post_id, user_id FROM feed_data_natalikuva_features_lesson_22"
        # LIMIT 100000 отрабатывает 0.28 сек
        # feed_data WHERE action='like' LIMIT 400000
        user_query = "SELECT * FROM user_features_natalikuva_features_lesson_22"

    # Загрузка с логированием
    #load_start = time.time()

    # Загрузка
    like_posts = batch_load_sql(likes_query)

    #log_execution_time("Загрузка лайков", load_start)

    #load_start = time.time()

    user_features = batch_load_sql(user_query).set_index('user_id')

    #log_execution_time("Загрузка пользователей", load_start)

    #load_start = time.time()

    posts_features = batch_load_sql(posts_query).set_index('post_id')

    #log_execution_time("Загрузка постов", load_start)

    #process_start = time.time()

    # Обработка результатов
    liked_posts_dict = (
        {user_id: set(like_posts['post_id'])}
        if user_id else
        {uid: set(group['post_id']) for uid, group in like_posts.groupby('user_id')}
    )

    # Проверка уникальности индексов
    posts_features = posts_features[~posts_features.index.duplicated(keep='first')]
    #log_execution_time("Обработка данных", process_start)
    #log_execution_time("Общее время load_features", start_time)

    return {
        'user_features': user_features,
        'posts_features': posts_features,
        'liked_posts_dict': liked_posts_dict
    }

model=load_models()
features =load_features()

def get_recommended_posts(
		id: int,
		time: datetime,
		limit: int = 5) -> List[PostGet]:

    #total_start = time.time()  # Общий замер времени

    try:
        # Логирование времени получения пользователя
        #user_start = time.time()
        user_features = features['user_features'].loc[id].to_dict()
        #log_execution_time("Получение данных пользователя", user_start)
    except KeyError:
        print(f"[WARNING] Пользователь {id} не найден")
        return []

    # Логирование подготовки данных
    #prep_start = time.time()

    # Рекомендации (топ-N постов)
    candidate_posts = features['posts_features'][model.feature_names_].copy()

    # Добавляем пользователя ко всем постам
    # user_features = features['user_features'].loc[id]
    for col, val in user_features.items():
        candidate_posts[col] = val

    # Проверяем порядок
    #candidate_posts = candidate_posts[model.feature_names_]

    # Удаляем дубликаты постов
    candidate_posts = candidate_posts[~candidate_posts.index.duplicated(keep='first')]

    #log_execution_time("Подготовка данных", prep_start)

    # Логирование предсказания
    #predict_start = time.time()

    # Предсказание
    predicts = model.predict_proba(candidate_posts)[:, 1]
    #log_execution_time("Предсказание модели", predict_start)

    # Логирование фильтрации
    #filter_start = time.time()
    liked_posts = features['liked_posts_dict'].get(id, set())
    mask = ~candidate_posts.index.isin(liked_posts)

    # маска к predicts
    top_posts = (
        candidate_posts[mask]
        .assign(predicts=predicts[mask])
        .nlargest(limit, 'predicts')
        .index
    )
    #log_execution_time("Фильтрация и сортировка", filter_start)

    # Логирование формирования ответа
    #result_start = time.time()
    # Ответ с проверкой
    posts_data = features['posts_features']

    result= [
        PostGet(
            id=int(post_id),
            text=str(posts_data.loc[post_id, 'text']),
            topic=str(posts_data.loc[post_id, 'topic'])
        )
        for post_id in top_posts
    ]
    #log_execution_time("Формирование результата", result_start)
    #log_execution_time("Общее время get_recommended_posts", total_start)

    return result


@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
		id: int,
		time: datetime,
		limit: int = 10) -> List[PostGet]:
     return get_recommended_posts(id,
                                  time,
                                  limit)


# функция для тестирования
#def test_all():
#    print("\nТест загрузки данных...")
#    global features
#    features = load_features()
#    check_memory_usage()

#    print("\nТест рекомендаций...")
#    test_users = features['user_features'].sample(100).index
#    total_time = 0

#    for user_id in test_users:
#        start = time.time()
#        recommendations = get_recommended_posts(user_id, datetime.now(), 5)
#        total_time += time.time() - start

#    avg_time = total_time / len(test_users)
#    print(f"Среднее время: {avg_time:.3f} сек")
#    print(f"Общее время: {total_time:.3f} сек")