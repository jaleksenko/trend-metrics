import random
import json
from datetime import datetime, timedelta

# Функция для генерации случайной даты
def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

# Функция для чтения текста из файла и его разбиения на предложения
def read_sentences_from_file(file_path, total_sentences):
    with open(file_path, 'r') as file:
        text = file.read()
    sentences = text.split('.')  # Разбиваем текст на предложения по точке
    return sentences[:total_sentences]  # Возвращаем заданное количество предложений

# Начальная и конечная даты для генерации
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 1, 2)

# Чтение предложений из файла
file_path = 'test.dat'  # Укажите путь к вашему файлу .dat
total_sentences = 1000  # Общее количество предложений для выбора
sentences = read_sentences_from_file(file_path, total_sentences)

# Генерация данных
data = []
for i in range(len(sentences)):  # Генерируем сообщения на основе предложений
    message = {
        "id": f"{random.randint(1, 10000)}",
        "title": sentences[i],
        "published": random_date(start_date, end_date).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ChannelTitle": "Channel " + str(random.randint(1, 5))
    }
    data.append(message)

# Сохранение данных в файл
with open('test_messages.json', 'w') as file:
    json.dump(data, file, indent=4)

print("Data generation completed.")
