import os
from catboost import CatBoostClassifier


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
    """
    Загружает и возвращает модель из файла
    """
    model_path = (get_model_path
                  ("C:/Users/natal/PycharmProjects/PythonProject/final_project_2/catboost_model.cbm")
                  #("/my/super/path/catboost_model.cbm")
                   )
    model = CatBoostClassifier()
    model.load_model(model_path)
    return model
