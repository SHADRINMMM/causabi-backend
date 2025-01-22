
from flask import Flask, request, jsonify, Blueprint 
from . import db  # Импорт вашей базы данных
from flask_login import UserMixin
from sqlalchemy.sql import func
from .models import User
import os 


# Разрешенный IP-адрес
ALLOWED_IP = os.getenv('ALLOWED_IP')  # Замените на нужный IP-адрес


auth = Blueprint('auth', __name__)
@auth.route('/create_user', methods=['POST'])
def create_user():
    # Проверка IP-адреса
    client_ip = request.remote_addr
    if client_ip != ALLOWED_IP:
        return jsonify({"error": "Access denied"}), 403  # 403 Forbidden

    # Получение данных из запроса
    data = request.json
    name = data.get('name')
    id = data.get('id')
    email = data.get('email')
    current_file_name = data.get('current_file_name', '')  # По умолчанию пустая строка

    # Проверка обязательных полей
    if not name or not email:
        return jsonify({"error": "Name and email are required"}), 400  # 400 Bad Request

    # Проверка уникальности email
    if User.query.filter_by(email=email).first():
        return jsonify({"error": "Email already exists"}), 409  # 409 Conflict

    # Создание нового пользователя
    new_user = User(
        id = id,
        name=name,
        email=email,
        current_file_name=current_file_name
    )

    # Добавление в базу данных
    db.session.add(new_user)
    db.session.commit()

    # Возврат успешного ответа
    return jsonify({"message": "User created successfully", "user_id": new_user.id}), 201  # 201 Created