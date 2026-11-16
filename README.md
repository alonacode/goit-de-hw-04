# goit-de-hw-04

Домашнє завдання: порівняння кількості Jobs у SparkUI для трьох варіантів одного коду
(без проміжної дії, з проміжним `collect()`, з `cache()`).

## Запуск

Потрібні Python 3 та Java 17 (`java -version`).

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python part1.py
```

Кожен скрипт після виконання зупиняється на `input()`, щоб можна було відкрити
SparkUI (http://localhost:4040/jobs/) і зробити скриншот. Enter завершує сесію Spark.
Скрипти запускаємо по одному, щоб порт 4040 не був зайнятий.

Дані: `nuek-vuh3.csv`.

## Скриншоти

| Частина | Файл | Jobs |
|---|---|---|
| 1. Без проміжної дії | `screenshots/part1.png` | 5 |
| 2. Проміжний `collect()` | `screenshots/part2.png` | 8 |
| 3. З `cache()` | `screenshots/part3.png` | 7 |

## Частина 1 — `part1.py`: 5 Jobs

- **Job 0 і 1 (`csv`)** — читання CSV. Через `inferSchema=true` Spark спочатку
  читає файл, щоб визначити типи колонок, тому на читання йде два Jobs.
- **Job 2, 3, 4 (`collect`)** — один виклик `collect()` дає три Jobs. У запиті є
  `repartition(2)` і `groupBy` (два shuffle), а Adaptive Query Execution виконує
  запит поетапно: після кожного shuffle планує наступний крок за статистикою.
  Кожен крок — окремий Job.

`where("count>2")` — це transformation, вона ліниво додається до плану й нових
Jobs не створює. Jobs створює лише action (`collect`).

Стовпець Stages показує «skipped» — це етапи, результат яких уже є у shuffle-файлах
попередніх Jobs, тому вони не перераховуються.

## Частина 2 — `part2.py`: 8 Jobs

Додано проміжний `collect()`: 2 (`csv`) + 3 (перший `collect`) + 3 (другий `collect`) = 8.

Чому на 3 Jobs більше: Spark не пам'ятає результат першого `collect()`. DataFrame
`nuek_processed` — це лише план обчислень. Після `.where("count>2")` другий
`collect()` знову виконує весь ланцюжок: читання, `repartition`, `where`, `select`,
`groupBy`, фільтр. Тобто ті самі три Jobs виконуються вдруге.

## Частина 3 — `part3.py`: 7 Jobs

Додано `.cache()` на результаті `groupBy().count()`: 2 (`csv`) + 3 (перший `collect`,
який обчислює й заповнює кеш) + 2 (другий `collect`) = 7.

Чому на 1 Job менше, ніж у частині 2: `cache()` зберігає обчислений DataFrame
в пам'яті після першого action. Другий `collect()` бере дані з кешу й лише застосовує
`where("count>2")`, а не рахує все з нуля (читання файлу, `repartition`, `groupBy`).
Саме `cache()` ліниве: нічого не зберігається, доки не виконано action,
тому перший `collect()` усе ще рахує весь ланцюжок.

## Що таке `cache()` і навіщо він потрібен

`cache()` позначає DataFrame/RDD для збереження в пам'яті (рівень `MEMORY_ONLY`
для RDD; для DataFrame за замовчуванням `MEMORY_AND_DISK`). Це корисно, коли той самий
проміжний результат використовується кількома actions: Spark не перераховує його,
а бере з кешу, тож швидкість зростає, а кількість Jobs і роботи зменшується.
`unpersist()` звільняє пам'ять, коли кеш більше не потрібен.
Для інших рівнів зберігання є `persist()`.
