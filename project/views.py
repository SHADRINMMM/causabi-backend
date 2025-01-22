import os
import re
import pandas as pd
import numpy as np

from flask import Blueprint, render_template, request, jsonify, make_response, redirect, url_for, stream_with_context,stream_template,Response,send_file,render_template_string
from flask_login import login_required, current_user, login_user
from .models import  User

from dotenv import load_dotenv
load_dotenv()


PATH_TO = os.getenv('PATH_TO')
ALLOWED_IP = os.getenv('ALLOWED_IP') 
SECRET_TOKEN = os.getenv('SECRET_TOKEN') 

views = Blueprint('views', __name__)
#выполнение кода python - часть анализ 
def executor_a_d(result_code: str, h: str, user_id:str ) -> str:
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
                corrected_code = corrected_code.replace(path, f"{PATH_TO}{user_id}/{filename}")
        
        # Выполнение исправленного кода
        loc = {}
        exec(corrected_code, globals(), loc)
        
        # Проверка, что переменная h существует
        if h not in loc:
            return f"Error: The variable '{h}' is missing in the generated code."
        
        k = loc[h]
        return str(k)
    except Exception as e:
        return f"Traceback (most recent call last):\n{str(e)}"
    

import re

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
                corrected_code = corrected_code.replace(path, f"{PATH_TO}{user_id}/{filename}")
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
    user = User.query.filter_by(id = user_id).first()
    if user is None: 
        return jsonify({"error": "User not found"}), 404

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
    user = User.query.filter_by(id = user_id).first()

    if user is None: 
        return jsonify({"error": "User not found"}), 404
    
    is_valid, error_message = check_file_names_in_code(result_code,  f"-{user_id}-")
    if not is_valid:
        return jsonify({"error": error_message}), 400

    # Вызываем функцию executor_a_d
    result = executor_a_d(result_code, h,  user_id)
    
    # Возвращаем результат
    return jsonify({"result": result})
