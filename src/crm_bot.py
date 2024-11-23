from telethon.sync import TelegramClient
import time
from src.crm import check_new, api_parsing, open_hashes, compare_images, update_hashes_all, SKIPPED_PAIRS
import asyncio
import traceback
from telethon import events
import os
from dotenv import load_dotenv

load_dotenv()
api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")


client = TelegramClient('bot', api_id, api_hash).start(bot_token=os.getenv("BOT_TOKEN"))


def build_graph(pairs):
    graph = {}
    for a, b in pairs:
        if a not in graph:
            graph[a] = []
        if b not in graph:
            graph[b] = []
        graph[a].append(b)
        graph[b].append(a)
    return graph


def find_components(graph):
    visited = set()
    components = []

    def dfs(node, component):
        visited.add(node)
        component.append(node)
        for neighbor in graph[node]:
            if neighbor not in visited:
                dfs(neighbor, component)

    for node in graph:
        if node not in visited:
            component = []
            dfs(node, component)
            components.append(sorted(component, key=int))
    return components


def save_active_users(active_users):
    with open(r'..\bot\active_users.txt', 'w') as file:
        for user in active_users:
            file.write(f"{user}\n")

def load_active_users():
    try:
        with open(r'..\bot\active_users.txt', 'r') as file:
            return set(int(line.strip()) for line in file if line.strip())
    except Exception:
        return set()
    


active_users = set()

# Состояние ожидания ввода от пользователя
waiting_for_clones = False

@client.on(events.NewMessage(pattern='/start'))
async def start_command(event):
    # Добавляем chat_id пользователя в список активных
    active_users.add(event.chat_id)
    save_active_users(active_users)
    await client.send_message(event.chat_id,'Привет! Бот активирован.')
    # await event.respond('Привет! Бот активирован.')


@client.on(events.NewMessage(pattern='/add'))
async def start_add_clones(event):
    if event.chat_id not in active_users:
        return
    global waiting_for_clones
    waiting_for_clones = True
    await event.respond('Пожалуйста, отправьте пару ID в формате "id1 id2".')
    
@client.on(events.NewMessage)
async def handle_message(event):
    global waiting_for_clones
    if waiting_for_clones and not event.message.text.startswith('/'):
        try:
            id1, id2 = event.message.text.split()
            with open(r'crm\clones.txt', 'a') as file:
                file.write(f"{id1} {id2}\n")
            await event.respond(f"Клоны {id1} и {id2} успешно добавлены.")
        except ValueError:
            await event.respond("Некорректный ввод. Убедитесь, что вы отправили два ID через пробел.")
        waiting_for_clones = False


def check_id_pair_in_file(id1, id2):
    with open(r'..\hash\clones.txt', 'r') as file:
        pairs = file.read().splitlines()
    pair = f"{id1} {id2}"
    reverse_pair = f"{id2} {id1}"
    return any(pair == line or reverse_pair == line for line in pairs)


@client.on(events.NewMessage(pattern='/list'))
async def list_duplicates(event):
    if event.chat_id not in active_users:
        return
    with open(r'..\hash\clones.txt', 'r') as file:
        pairs = [line.strip().split() for line in file]
    
    graph = build_graph(pairs)
    components = find_components(graph)
    
    messages = []
    current_message = ""
    link_count = 0
    base_url = "https://rnb-crm.app/objects/"
    
    for component in components:
        links = [f"[ID{node}]({base_url}{node})" for node in component]
        component_message = ", ".join(links) + "\n\n"
        
        # Проверяем, сколько ссылок содержится в текущем сообщении
        if link_count + len(links) > 90:  # Отправляем текущее сообщение, если добавление этой компоненты превысит 90 ссылок
            messages.append(current_message)
            current_message = component_message
            link_count = len(links)
        else:
            current_message += component_message
            link_count += len(links)

    if current_message:
        messages.append(current_message)

    # Отправляем все собранные сообщения
    for msg in messages:
        await event.respond(msg, parse_mode='markdown')



@client.on(events.NewMessage(pattern='/delete'))
async def delete_clones(event):
    if event.chat_id not in active_users:
        return

    try:
        ids_to_delete = event.message.text.split()[1:]  # Получаем ID из сообщения
        if len(ids_to_delete) != 2:
            raise ValueError("Нужно отправить ровно два ID.")
        
        id1, id2 = ids_to_delete
        found = False

        with open(r'..\hash\clones.txt', 'r+') as file:
            lines = file.readlines()
            file.seek(0)
            for line in lines:
                pair = line.strip().split()
                if (id1 in pair and id2 in pair) and (pair.index(id1) != pair.index(id2)):
                    found = True
                else:
                    file.write(line)
            file.truncate()  # Удалить не записанное в файл содержимое
        
        if not found:
            raise ValueError("Пара не найдена.")

        await event.respond(f"Пара {id1} и {id2} успешно удалена.")
    except Exception as e:
        await event.respond(f"Ошибка: {str(e)}")


@client.on(events.NewMessage(pattern='/check'))
async def differences(event):
    if event.chat_id not in active_users:
        return
    start = time.time()

    check = api_parsing()
    
    update_hashes_all(list(check))

    hashes = open_hashes('hash')

    data_list = compare_images(hashes, skipped_pairs=SKIPPED_PAIRS)
    
    hash_plans = open_hashes('hash_plans')
    
    data_list.extend(compare_images(hash_plans, skipped_pairs=SKIPPED_PAIRS))
    
    for data in data_list:
        types, urls = data
        ids = [x.split(': ')[1] for x in types]
        
        # Проверка на наличие пар в файле
        if check_id_pair_in_file(ids[0], ids[1]):
            continue
        
        type_names = [x.split(': ')[0] for x in types]
        type_name = 'блока' if type_names[0] == 'block' else 'здания'
        if type_names[0] != type_names[1]:
            type_name = 'блоков и зданий'
        
        url_1 = 'https://rnb-crm.app/blocks/' + ids[0] if type_names[0] == 'block' else 'https://rnb-crm.app/objects/' + ids[0]
        url_2 = 'https://rnb-crm.app/blocks/' + ids[1] if type_names[1] == 'block' else 'https://rnb-crm.app/objects/' + ids[1]
        
        
        message = f"Внимание! Найдены дубликаты фото у {type_name} [ID{ids[0]}]({urls[0]}) и [ID{ids[1]}]({urls[1]})"
        
        hypermessage = f'{message}\n Ссылки: [первая]({url_1}) [вторая]({url_2})'
        
        await client.send_message(event.chat_id,hypermessage, parse_mode='markdown')
        await client.send_message(event.chat_id, 'ждём', parse_mode='markdown')

    print('done!')

    finish = time.time()
    print(finish - start, 'seconds')
    


async def periodic():
    while True:
        if not active_users:  # Проверяем, есть ли активные пользователи
            await asyncio.sleep(10)  # Короткое ожидание перед следующей проверкой
            continue
        
        try:
            # print(active_users)
            start = time.time()
            # for chat_id in active_users:
                # await client.send_message(chat_id,hypermessage, parse_mode='markdown')
            # await client.send_message(chat_id, hypermessage, parse_mode='markdown')
                # await client.send_message(chat_id, 'ждём', parse_mode='markdown')
            data_list = check_new()
            
            # for chat_id in active_users:
            #     await client.send_message(chat_id, str(data_list), parse_mode='markdown')
            
            # print(len(data_list))
            
            for data in data_list:
                types, urls = data
                ids = [x.split(': ')[1] for x in types]
                
                # Проверка на наличие пар в файле
                if check_id_pair_in_file(ids[0], ids[1]):
                    continue
                
                type_names = [x.split(': ')[0] for x in types]
                type_name = 'блока' if type_names[0] == 'block' else 'здания'
                if type_names[0] != type_names[1]:
                    type_name = 'блоков и зданий'
                
                url_1 = 'https://rnb-crm.app/blocks/' + ids[0] if type_names[0] == 'block' else 'https://rnb-crm.app/objects/' + ids[0]
                url_2 = 'https://rnb-crm.app/blocks/' + ids[1] if type_names[1] == 'block' else 'https://rnb-crm.app/objects/' + ids[1]
                
                
                message = f"Внимание! Найдены дубликаты фото у {type_name} [ID{ids[0]}]({urls[0]}) и [ID{ids[1]}]({urls[1]})"
                
                hypermessage = f'{message}\n Ссылки: [первая]({url_1}) [вторая]({url_2})'
                for chat_id in active_users:
                    await client.send_message(chat_id,hypermessage, parse_mode='markdown')
                # await client.send_message(chat_id, hypermessage, parse_mode='markdown')
                    await client.send_message(chat_id, 'ждём', parse_mode='markdown')
            execution_time = time.time() - start
            print(execution_time, 'seconds')
            sleep_duration = max(120 - execution_time, 0)
            clear = lambda: os.system('clear')
            clear()
            # print('done!')
            await asyncio.sleep(sleep_duration)  # 600 секунд = 10 минут
            # print('я поспал')
        except Exception as e:
            print('Ошибка:', str(e))
            traceback.print_exc()  # Печатаем полную трассировку стека ошибок для диагностики
            await asyncio.sleep(120)

# client.loop.run_until_complete(periodic())

asyncio.ensure_future(periodic())

client.start()
client.run_until_disconnected()