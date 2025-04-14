"""
Программа для обработки дерева комментариев в формате JSON.

Выполняемые шаги:
1. Загрузка текстов комментариев из внешнего API
2. Рекурсивная обработка дерева
3. Добавление текстов комментариев к исходным идентификаторам
4. Вывод результата в формате JSON

"""
import sys
import json
import aiohttp
import asyncio

async def Com_load(session: aiohttp.ClientSession, num_comment: int) -> dict:
    """
    Функция асинхронно загружает данные одного комментария по его номеру.

    session (aiohttp.ClientSession): сессия для HTTP-запросов
    num_comment (int): уникальный идентификатор комментария

    Функция возвращает - словарь с данными комментария вида {'id': int, 'body': str}.

    aiohttp.ClientError: в случае проблем с загрузкой
    ValueError: если API вернул неверные данные
    """
    url = f"https://winry.khashaev.ru/posts/{num_comment}"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.text()
                try:
                    transform = json.loads(data)
                    if not isinstance(transform, dict):
                        raise ValueError("Неверное, возващаемое API, значение")
                    return transform
                except (json.JSONDecodeError, ValueError) as e:
                    print(f"Неверный JSON для комментария {num_comment}: {e}", 
                          file=sys.stderr)
                    return {
                        "id": num_comment,
                        "body": data.strip('"')
                    }
            return {
                "id": num_comment,
                "body": None
            }
            
    except Exception as error:
        print(f"Ошибка при загрузке {num_comment}: {error}", 
              file=sys.stderr)
        return {
            "id": num_comment,
            "body": None
        }

async def Com_process(session: aiohttp.ClientSession, comment: dict) -> dict:
    """
    Рекурсивно обрабатывает дерево, добавляя тексты.

    session (aiohttp.ClientSession) - Сессия для HTTP-запросов
    comment (dict) - узел дерева комментариев

    Функция возвращает обработанное дерево с добавленными комментариями.

    Ход работы:
    1. Параллельно загружает текст текущего комментария
    2. Рекурсивно обрабатывает ответы 
    3. Объединяет результаты в одну структуру
    """
    cur_com = asyncio.create_task(Com_load(session, comment["id"]))
    
    task_lst = []
    if "replies" in comment and comment["replies"]:
        for reply in comment["replies"]:
            task = asyncio.create_task(Com_process(session, reply))
            task_lst.append(task)
    
    results = await asyncio.gather(cur_com, *task_lst)
    cur_result = results[0]
    rest_result = results[1:]
    
    return {
        "id": comment["id"],
        "body": cur_result.get("body") or cur_result.get("text"),
        "replies": rest_result if task_lst else []
    }

async def main():
    """
    Основная функция для обработки ввода/вывода.

    Читает JSON через стандартный ввод stdin
    Выводит обработанное дерево комментариев в stdout

    json.JSONDecodeError: если ввод не верный
    Exception: иные ошибки выполнения
    """
    try:
        imp_json = sys.stdin.read()
        data_json = json.loads(imp_json)
        
        async with aiohttp.ClientSession() as session:
            answer = await Com_process(session, data_json)
            print(json.dumps(answer, indent=2, ensure_ascii=False))
            
    except json.JSONDecodeError as e:
        print(f"Ошибка: Неверный JSON на входе - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Критическая ошибка: {e}", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    """
    Точка входа.
    Запускает асинхронную main() функцию.
    """
    asyncio.run(main())
