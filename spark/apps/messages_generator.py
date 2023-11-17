import random
import json
from datetime import datetime, timedelta

# Функция для генерации случайной даты
def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

# Начальная и конечная даты для генерации
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 1, 2)

# Список примеров заголовков
titles = [
    "Big Data Analysis Techniques",
    "Introduction to Machine Learning",
    "Advanced Deep Learning Models",
    "Data Engineering Best Practices",
    "Latest Trends in AI",
    "Exploring Neural Networks",
    "Cloud Computing Innovations",
    "Quantum Computing in Modern World",
    "Blockchain Technology Overview",
    "Cybersecurity and Data Protection",
    "Augmented Reality in Education",
    "Virtual Reality in Gaming",
    "Internet of Things Applications",
    "5G Network Capabilities",
    "Sustainable Energy Solutions",
    "Biotechnology in Healthcare",
    "Nanotechnology in Manufacturing",
    "Robotics in Industry 4.0",
    "Space Exploration and Technology",
    "Artificial Intelligence in Finance"
]


# Генерация данных
data = []
for _ in range(1000):  # Генерируем 1000 сообщений
    message = {
        "id": f"{random.randint(1, 10000)}",
        "title": random.choice(titles),
        "published": random_date(start_date, end_date).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ChannelTitle": "Channel " + str(random.randint(1, 5))
    }
    data.append(message)

# Сохранение данных в файл
with open('test_messages.json', 'w') as file:
    json.dump(data, file, indent=4)

print("Data generation completed.")
