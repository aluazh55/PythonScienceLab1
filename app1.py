from flask import Flask, request, jsonify
import psycopg2
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Error as PsycopgError

load_dotenv()

app = Flask(__name__)

# Настройки БД
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "bank",
    "user": "postgres",
    "password": "postgres"
}


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


@app.route('/report', methods=['GET'])

def get_report():
    # Получаем параметры из запроса
    account_number = request.args.get('account')
    report_type    = request.args.get('type')
    start_date     = request.args.get('start')
    end_date       = request.args.get('end')

    # Проверка наличия всех обязательных параметров
    if not all([account_number, report_type, start_date, end_date]):
        return jsonify({
            "error": "Все параметры обязательны: account, type, start, end"
        }), 400

    # Проверка допустимого типа отчёта
    if report_type not in {'daily', 'monthly'}:
        return jsonify({
            "error": "Параметр type должен быть 'daily' или 'monthly'"
        }), 400

    # Определяем имя функции в базе данных
    db_function = "daily_report" if report_type == "daily" else "monthly_report"

    conn = None
    cur = None

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Безопасная подстановка имени функции (f-строка здесь допустима,
        # т.к. мы контролируем значение db_function)
        query = f"SELECT * FROM {db_function}(%s, %s, %s)"

        # Выполняем запрос с параметризацией (защита от SQL-инъекций)
        cur.execute(query, (account_number, start_date, end_date))

        # Получаем имена колонок
        columns = [desc[0] for desc in cur.description]

        # Получаем все строки
        rows = cur.fetchall()

        # Преобразуем в список словарей
        data = [dict(zip(columns, row)) for row in rows]

        # Формируем ответ
        return jsonify({
            "account": account_number,
            "type": report_type,
            "period": {
                "start": start_date,
                "end": end_date
            },
            "data": data
        })

    except PsycopgError as db_err:
        # Более точная обработка ошибок базы данных
        return jsonify({
            "error": "Ошибка базы данных",
            "detail": str(db_err)
        }), 500

    except Exception as e:
        # Общая ошибка (для отладки лучше логировать, а не отдавать клиенту)
        return jsonify({
            "error": "Внутренняя ошибка сервера",
            "detail": str(e)   # ← в продакшене это лучше убрать или логировать
        }), 500

    finally:
        # Гарантированное закрытие соединения и курсора
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True, port=5001)