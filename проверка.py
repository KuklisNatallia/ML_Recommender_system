from app import app
from fastapi.testclient import TestClient
from datetime import datetime

client = TestClient(app)

user_id = 204
time = datetime(2021, 12, 15).isoformat()

try:
    r = client.get(
        "/post/recommendations/",
        params={
            "id": user_id,  # ID пользователя
            "time": time,
            "limit": 5
        }
    )
    r.raise_for_status()  # Проверка на ошибки HTTP
except Exception as e:
    raise ValueError(f"Ошибка при выполнении запроса: {type(e)} {str(e)}")

print(r.json())
# Укажите URL вашего сервиса
#url = "http://localhost:8000/post/recommendations/"

# Отправка GET-запроса
#response = requests.get(url, params=params)

# Проверка статуса и вывод результата
#if response.status_code == 200:
#    recommendations = response.json()
#    print("Рекомендованные посты:", recommendations)
#else:
#    print("Ошибка:", response.status_code, response.text)

#print(features[1].columns)  # Какие столбцы есть в posts_features
#print(features[2].columns)

