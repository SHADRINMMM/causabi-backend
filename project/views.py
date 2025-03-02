import os
import re
import pandas as pd
import numpy as np

from flask import Blueprint, json,  request,jsonify#,render_template, jsonify, make_response, redirect, url_for, stream_with_context,stream_template,Response,send_file,render_template_string
#from flask_login import login_required, current_user, login_user
from .models import  User

from dotenv import load_dotenv
load_dotenv()


PATH_TO = os.getenv('PATH_TO')
ALLOWED_IP = os.getenv('ALLOWED_IP') 
SECRET_TOKEN = os.getenv('SECRET_TOKEN') 

views = Blueprint('views', __name__)
#выполнение кода python - часть анализ 
def executor_a_d(result_code: str, h: str, user_id: str) -> dict:
    """
    Выполняет переданный код и возвращает результат в унифицированном формате.

    Параметры:
    - result_code: Код на Python для выполнения.
    - h: Имя переменной, которую нужно вернуть.
    - user_id: ID пользователя для корректировки путей к файлам.

    Возвращает:
    - Словарь с ключами "type" и "data":
        - "type": Тип результата ("dataframe", "string", "error").
        - "data": Данные (DataFrame, строка или сообщение об ошибке).
    """
    try:
        # Находим все пути к файлам в коде
        file_paths = re.findall(r"[\'\"](.*?\.(csv|json|parquet))[\'\"]", result_code)
        
        # Заменяем некорректные пути на корректные
        corrected_code = result_code
        for path, ext in file_paths:
            if not path.startswith(PATH_TO):
                # Извлекаем имя файла (последнюю часть пути)
                filename = path.split("/")[-1]
                # Заменяем путь на корректный
                corrected_code = corrected_code.replace(path, f"{PATH_TO}/{user_id}/{filename}")
        
        # Выполнение исправленного кода
        loc = {}
        exec(corrected_code, globals(), loc)
        
        # Проверка, что переменная h существует
        if h not in loc:
            return {"type": "error", "data": f"The variable '{h}' is missing in the generated code."}
        
        k = loc[h]
        
        # Определяем тип результата
        if isinstance(k, pd.DataFrame):
            return {"type": "dataframe", "data": k.to_dict(orient="records")}
        elif isinstance(k, (str, int, float, list, dict)):
            return {"type": "string", "data": str(k)}
        else:
            return {"type": "error", "data": f"Unsupported data type: {type(k)}"}
    
    except Exception as e:
        return {"type": "error", "data": f"Traceback (most recent call last):\n{str(e)}"}    

def check_file_names_in_code(code: str, substring: str) -> tuple[bool, str]:
    """
    Проверяет, что все файлы, упоминаемые в коде, содержат подстроку в названии.

    :param code: Исходный код в виде строки.
    :param substring: Подстрока, которая должна быть в названии файла.
    :return: Кортеж (bool, str):
            - True, если все файлы содержат подстроку.
            - False и сообщение об ошибке, если какой-то файл не содержит подстроку.
    """
    # Находим все пути к файлам в коде
    file_paths = re.findall(r"[\'\"](.*?\.(csv|json|parquet|txt|py))[\'\"]", code)
    
    # Проверяем каждый путь
    for path, _ in file_paths:
        # Извлекаем имя файла (последнюю часть пути)
        filename = path.split("/")[-1]
        
        # Проверяем, содержит ли имя файла подстроку
        if substring not in filename:
            return False, f"File '{filename}' does not contain the required substring '{substring}'."
    
    # Если все файлы содержат подстроку
    return True, "All file names are valid."


##visualization and data requests

def executor_a_d_vis(result_code: str, h: str, user_id:str ) -> pd.DataFrame:
    """
    Выполняет переданный код, исправляет пути к файлам и возвращает объект pandas DataFrame.

    Параметры:
    - result_code: Строка с кодом на Python.
    - h: Имя переменной, которая содержит DataFrame.

    Возвращает:
    - pandas DataFrame, если выполнение прошло успешно.
    - Если возникает ошибка, возвращает DataFrame с сообщением об ошибке.
    """
    try:
        # Находим все пути к файлам в коде
        file_paths = re.findall(r"[\'\"](.*?\.(csv|json|parquet))[\'\"]", result_code)
        
        # Заменяем некорректные пути на корректные
        corrected_code = result_code
        for path, ext in file_paths:
            if not path.startswith(PATH_TO):
                # Извлекаем имя файла (последнюю часть пути)
                filename = path.split("/")[-1]
                # Заменяем путь на корректный
                corrected_code = corrected_code.replace(path, f"{PATH_TO}/{user_id}/{filename}")
                #corrected_code = corrected_code.replace(path, f"{filename}")
        
        # Локальное пространство имен для выполнения кода
        loc = {}
        
        # Выполняем исправленный код
        exec(corrected_code, globals(), loc)
        
        # Проверяем, что переменная h существует
        if h not in loc:
            return pd.DataFrame({"error": [f"The variable '{h}' is missing in the generated code."]})
        
        # Получаем объект по имени переменной h
        k = loc[h]
        
        # Проверяем, является ли объект DataFrame
        if isinstance(k, pd.DataFrame):
            return k
        else:
            return pd.DataFrame({"error": ["The variable is not a pandas DataFrame."]})
    
    except Exception as e:
        # В случае ошибки возвращаем DataFrame с сообщением об ошибке
        return pd.DataFrame({"error": [f"Traceback (most recent call last):\n{str(e)}"]})

def get_tables_info(list_file_name, user_id):
    """
    Собирает информацию о таблицах из списка CSV-файлов.

    Параметры:
    list_file_name (list): Список путей к CSV-файлам.

    Возвращает:
    str: JSON-строка с информацией о таблицах.
    """
    tables_info = []

    for file_name in list_file_name:
        try:
            df = pd.read_csv(f"{PATH_TO}/{user_id}/{file_name}")

            if df.empty:
                table_info = {
                    "file_name": file_name,
                    "num_rows": 0,
                    "num_columns": 0,
                    "columns": [],
                    "dtypes": {},
                    "missing_values": {},
                    "column_details": {}
                }
                tables_info.append(table_info)
                continue

            table_info = {
                "file_name": file_name,
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "dtypes": df.dtypes.astype(str).to_dict(),
                "missing_values": df.isnull().sum().to_dict(),
                "column_details": {}
            }

            for column in df.columns:
                col_info = {}
                dtype = str(df[column].dtype)
                if 'int' in dtype:
                    col_info['type'] = 'integer'
                    col_info['range'] = (int(df[column].min()), int(df[column].max()))
                    if len(df) > 5:
                        col_info['examples'] = df[column].dropna().sample(min(5, len(df))).tolist()
                    else:
                        col_info['examples'] = df[column].dropna().tolist()

                elif 'datetime' in dtype:
                    col_info['type'] = 'datetime'
                    col_info['range'] = (df[column].min().strftime('%Y-%m-%d %H:%M:%S'),
                                        df[column].max().strftime('%Y-%m-%d %H:%M:%S'))
                    if len(df) > 5:
                        col_info['examples'] = df[column].dropna().sample(min(5, len(df))).dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
                    else:
                        col_info['examples'] = df[column].dropna().dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
                elif 'object' in dtype:
                    col_info['type'] = 'string'
                    if len(df) > 5:
                        col_info['examples'] = df[column].dropna().sample(min(5, len(df))).tolist()
                    else:
                        col_info['examples'] = df[column].dropna().tolist()
                else:
                    print(f"Необработанный тип данных {dtype} для столбца {column}")

                table_info['column_details'][column] = col_info

            tables_info.append(table_info)

        except Exception as e:
            table_info = {
                "file_name": file_name,
                "error": str(e)
            }
            tables_info.append(table_info)


    tables_info_json = json.dumps(tables_info, indent=4, ensure_ascii=False) 
    return tables_info_json


def get_csv_files(directory):
    """Gets a list of CSV files in the specified directory."""
    try:
        # Use os.listdir to get all files and directories in the specified directory
        all_files = os.listdir(directory)
        # Filter the list to include only files ending with .csv
        csv_files = [f for f in all_files if f.endswith('.csv') and os.path.isfile(os.path.join(directory, f))]
        return csv_files
    except FileNotFoundError:
        return []  # Return empty list if directory doesn't exist
    except OSError as e:  #Catch general OS errors.
        print(f"Error reading directory {directory}: {e}")
        return []

@views.route('/get-tables-info', methods=['POST'])
def get_info():
    token = request.headers.get("Authorization")
    print(1)
    
    # Проверяем токен
    if token != SECRET_TOKEN:
        return jsonify({"error": "Access denied. Invalid token."}), 403

    # Получаем данные из запроса
    data = request.json


    
    # Извлекаем параметры
    user_id = data['user_id']
    file_list = get_csv_files(f"{PATH_TO}/{user_id}")
    
    tables_info_json = get_tables_info(file_list, user_id)

    return jsonify({"result": tables_info_json})




@views.route('/execute-python-vis', methods=['POST'])
def execute_visualization():
    token = request.headers.get("Authorization")

    
    # Проверяем токен
    if token != SECRET_TOKEN:
        return jsonify({"error": "Access denied. Invalid token."}), 403

    # Получаем данные из запроса
    data = request.json
    
    # Проверяем, что все необходимые параметры переданы
    if not data or 'result_code' not in data or 'h' not in data:
        return jsonify({"error": "Missing 'result_code' or 'h' in request"}), 400
    
    # Извлекаем параметры
    result_code = data['result_code']
    h = data['h']
    user_id = data['user_id']
    if not os.path.isdir(f'{PATH_TO}/{user_id}'):
        return jsonify({"error": f"User not found {user_id}"}), 404

    is_valid, error_message = check_file_names_in_code(result_code, f"-{user_id}-")
    if not is_valid:
        return jsonify({"error": error_message}), 400
    
    #  обновляем данные пользователя 
    # запросить функцию для обновления данных пользователя - условие, в названии должна быть подстрока -id-
    
    # Вызываем функцию executor_a_d
    result = executor_a_d_vis(result_code, h, user_id)
    
    # Возвращаем результат
    if "error" in result.columns:
        return jsonify({"error": result["error"].iloc[0]}), 400
    return jsonify({"result": result.to_dict(orient="records")})

@views.route('/execute-python-analysis', methods=['POST'])
def execute_analysis():

    token = request.headers.get("Authorization")
    
    
    # Проверяем токен
    if token != SECRET_TOKEN:
        return jsonify({"error": "Access denied. Invalid token."}), 403

    # Получаем данные из запроса
    data = request.json
    
    # Проверяем, что все необходимые параметры переданы
    if not data or 'result_code' not in data or 'h' not in data:
        return jsonify({"error": "Missing 'result_code' or 'h' in request"}), 400
    
    # Извлекаем параметры
    result_code = data['result_code']
    h = data['h']
    user_id = data['user_id']
    if not os.path.isdir(f"{PATH_TO}/{user_id}"):
        return jsonify({"error": f"User not found {user_id}"}), 404
    
    is_valid, error_message = check_file_names_in_code(result_code,  f"-{user_id}-")
    if not is_valid:
        return jsonify({"error": error_message}), 400

    # Вызываем функцию executor_a_d
    result = executor_a_d(result_code, h,  user_id)
    
    # Возвращаем результат в зависимости от типа
    if result["type"] == "error":
        return jsonify({"error": result["data"]}), 400
    else:
        return jsonify({"result": result["data"]})
