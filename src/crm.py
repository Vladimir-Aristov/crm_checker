from PIL import Image
import requests
from tqdm import tqdm
import numpy as np
from numba import prange
import json
from numba import njit
from concurrent.futures import ThreadPoolExecutor, as_completed
import collections
from io import BytesIO
import traceback
import os
from dotenv import load_dotenv

load_dotenv()
API_URL = os.getenv("URL")

building_hash = '255255255255255255255255255255255255255255255255255255255255254255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255254255255254254254255255254255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255253251240250251253253255255253254255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255253253253253253253218194175242234219246236213253251243255255255254254253255255255255255255255255255255255255255255255255255255255255255253253253242243244244241234213187160245221203222193169220212204255254254243243243252252252255255255255255255255255255255255255255255255255255255250250249234234236199199191211192142247235219221199159181170156242240243240240240250250250255255255255255255255255255255255255255255255255255255250250250234235237205203194228205156237244255233219202210191165216221225234232231252252252255255255255255255255255255255255255255255255255255255251251250232234234227225224118108091224217200238225197255233182205211220234241241253254254255255255255255255255255255255255255255255255255255255250251251232226224214211212080092099088095091097101095182183175219209209201140130252244243255255255255254254255255255255255255255255255255255255251253253235224222212203202131139142083093100106114120180187191224218217131109108250246246255255255254254254255255255255255255255255255255255255253253253255255255218221223153158161113120124157162164110117120162169172254255255253254254255255255255255255255255255255255255255255255255255255254255255255255255240241241219220222190195198224226228164168171154159162255254254253253253255255255255255255255255255255255255255255255255255255254254254250250250255255255255255255255255255255255255255255255255255255254254254255255255255255255255255255255255255255255255255255255255255255252252253209208213206207211208208213218218222215215219207207212212212216216216220251251252255255255255255255255255255255255255255255255255255255255255255247247247245245246249249249250250251248248249247247248250250251250250250255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255255'


def hash_to_3d_array(hash_string):
    """
    Преобразует строку-хэш в трехмерный массив.
    
    :param hash_string: строка с хэшем (строка чисел, где каждые 3 символа — значение)
    :return: трехмерный массив NumPy
    """
    # Преобразование строки в список чисел
    flat_array = [int(hash_string[i:i+3]) for i in range(0, len(hash_string), 3)]
    
    # Определение размера массива (в данном случае квадратный)
    size = int(len(flat_array) // 3) ** 0.5
    size = int(size)
    
    # Преобразование в трехмерный массив
    array_3d = np.array(flat_array).reshape((size, size, 3))
    return array_3d

building_array = hash_to_3d_array(building_hash)


def load_skipped_pairs(filename):
    with open(filename, 'r') as file:
        skipped_pairs = set(tuple(map(int, line.split())) for line in file)
    return skipped_pairs                                                


SKIPPED_PAIRS = load_skipped_pairs(
        r'..\hash\clones.txt')


Image.MAX_IMAGE_PIXELS = None



def load_skipped_pairs(filename):
    skipped_pairs = []
    with open(filename, 'r') as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) == 2:
                skipped_pairs.append(tuple(parts))
    return skipped_pairs


def link_to_image(url):
    try:
        response = requests.get(url)
        img = Image.open(BytesIO(response.content)).convert('RGB')
    except Exception:
        img = None
    return img


def process_image(key, link):
    try:
        img = link_to_image(link)
        # print(img)
        if img is None:
            return None
        img_small = img.resize((16, 16), Image.LANCZOS)

        img.close()

        img_data = np.array(img_small).flatten()
        # print(img_data)
        img_small.close()

        prefix = '0' if link[41] == 'l' else '1'
        if 'wm' in link:
            number = str(key)
        else:
            number = link[46:link.find('/', 46)] if prefix == '0' else link[49:link.find('/', 49)]
        prefix += str(int(number)).zfill(4)

        hash_string = ''.join(str(pixel).zfill(3) for pixel in img_data)
        return f"{str(key).zfill(4)}_{prefix}_{hash_string}_{link}"
    except Exception as e:
        # print(f"Error processing image with key {key}: {e}")
        print(traceback.format_exc())
        return None


@njit()
def difference(img1, img2):
    """Find the difference between two images."""
    width = img1.shape[0]
    height = img1.shape[1]

    acc_r = 0.0
    acc_g = 0.0
    acc_b = 0.0

    for i in prange(width):
        for j in prange(height):
            acc_r += abs(img1[i, j, 0] - img2[i, j, 0])
            acc_g += abs(img1[i, j, 1] - img2[i, j, 1])
            acc_b += abs(img1[i, j, 2] - img2[i, j, 2])

    total_pixels = width * height
    average_diff_r = acc_r / total_pixels
    average_diff_g = acc_g / total_pixels
    average_diff_b = acc_b / total_pixels

    normalised_diff_r = average_diff_r / 255
    normalised_diff_g = average_diff_g / 255
    normalised_diff_b = average_diff_b / 255

    return (normalised_diff_r ** 2 + normalised_diff_g ** 2 + normalised_diff_b ** 2) ** 0.5


@njit
def str_to_int(s):
    final_index, result = len(s) - 1, 0
    for i, v in enumerate(s):
        result += (ord(v) - 48) * (10 ** (final_index - i))
    return result


@njit
def hash_to_matrix(line):
    id = str_to_int(line[0:4])

    info = ''

    bOrB = str_to_int(line[5])
    if bOrB == 0:
        info = f'block: {str_to_int(line[6:10])}'

    else:
        info = f'building: {str_to_int(line[6:10])}'

    nums = []
    for i in range(11, 2313, 3):
        word = str_to_int(line[i:i + 3])
        nums.append(word)

    size = int(np.sqrt(len(nums) / 3))

    matrix = np.array(nums).reshape((size, size, 3))
    link = line[2316:]
    return id, matrix, info, link


@njit(parallel=True, nogil=True)
def compare_images(hashes, skipped_pairs, threshold=0.005):
    duplicates = []
    matrices = []
    duplicates_links = []

    for i in range(len(hashes)):
        id, matrix, info, link = hash_to_matrix(hashes[i])
        matrices.append((id, matrix, info, link))


    for i in range(len(matrices)):
        id1, matrix1, info1, link1 = matrices[i]
        for j in prange(i + 1, len(matrices)):
            id2, matrix2, info2, link2 = matrices[j]

            if (id1, id2) in skipped_pairs or (id2, id1) in skipped_pairs:
                continue
            
            if np.array_equal(matrices[i][1], building_array) and np.array_equal(matrices[j][1], building_array):
                continue

            diff = difference(matrix1, matrix2)

            if diff < threshold and id1 != id2:
                link_pair = (link1, link2)
                duplicate_pair = (info1, info2)
                if duplicate_pair not in duplicates:
                    duplicates.append(duplicate_pair)
                    duplicates_links.append(link_pair)

    final_list = [*zip(duplicates, duplicates_links)]

    return final_list


@njit(parallel=True, nogil=True)
def compare_object(id, hashes, skipped_pairs, threshold=0.005):
    duplicates = []
    matrices = []
    duplicates_links = []
    object_pics = []

    for i in range(len(hashes)):
        id_, matrix, info, link = hash_to_matrix(hashes[i])
        matrices.append((id_, matrix, info, link))
        if id_ == id:
            object_pics.append((id, matrix, info, link))

    for i in prange(len(matrices)):
        id1, matrix1, info1, link1 = matrices[i]
        for j in prange(len(object_pics)):
            id2, matrix2, info2, link2 = object_pics[j]

            if (id1, id2) in skipped_pairs or (id2, id1) in skipped_pairs:
                continue

            diff = difference(matrix1, matrix2)

            if diff < threshold and id1 != id2:
                link_pair = (link1, link2)
                duplicate_pair = (info1, info2)
                if duplicate_pair not in duplicates:
                    duplicates.append(duplicate_pair)
                    duplicates_links.append(link_pair)

    final_list = [*zip(duplicates, duplicates_links)]

    return final_list


def api_parse_by_id(id):
    url = f'{API_URL}/objects/{id}'
    response = requests.get(url)
    data = response.json()

    photos = []
    plans = []
    for block in data['blocks']:
        for pic in block['pics']:
            if not pic["isPlan"]:
                photos.append(pic['url'])
            else:
                plans.append(pic['url'])
    for pic in data['pics']:
        if not pic["isPlan"]:
            photos.append(pic['url'])
        else:
            plans.append(pic['url'])

    return id, photos, plans


def parse_last():
    url = f'{API_URL}/buildings/last'
    response = requests.get(url)

    ids_list = response.json()

    id_photos = [api_parse_by_id(id_) for id_ in tqdm(ids_list, leave=False, desc='parse by id')]

    with open(r'..\db\objects.json', 'r') as fp:
        data_json = json.load(fp)

    with open(r'..\db\object_plans.json', 'r') as fp:
        plan_json = json.load(fp)

    for triple in tqdm(id_photos, leave=False, desc='triple'):
        data_json[str(triple[0])] = triple[1]
        plan_json[str(triple[0])] = triple[2]

    with open(r'..\db\objects.json', 'w') as fp:
        json.dump(data_json, fp)

    with open(r'..\db\object_plans.json', 'w') as fp:
        json.dump(plan_json, fp)

    update_hashes_all(ids_list)

    return ids_list


def update_hashes_all(ids_list):
    # Read all hashes and plans once
    hashes = open_hashes('hash')
    hash_plans = open_hashes('hash_plans')
    
    ids_begin = [str(id).zfill(4) for id in ids_list]

    data = open_json('objects')
    data_plans = open_json('object_plans')
    
    data_ids = list(data.keys())
    data_ids.extend(list(data_plans.keys()))
    object_ids = list(set(data_ids))
    print('num of objs:', len(object_ids))

    # Prepare new lists for hashes and plans
    print('len hashes:', len(hashes))
    hashes_n = [x for x in hashes if not x.startswith(tuple(ids_begin))]
    print('len hashes_new:', len(hashes_n))
    hash_plans_n = [x for x in hash_plans if not x.startswith(tuple(ids_begin))]
    print('len plan_hashes_new:', len(hash_plans_n))
    
    hashes_new = [x for x in hashes_n if str(int(x[:4])) in object_ids]
    hash_plans_new = [x for x in hash_plans_n if str(int(x[:4])) in object_ids]
    print('len ids_list', len(ids_list))

    # Process each ID
    for object_id in tqdm(ids_list, leave=False, desc='hashes'):
        id_str = str(object_id).zfill(4)
        print('t')
        # Extend the hashes and plans with new processed images
        links_small = {str(object_id): data[str(object_id)]}
        link_plans_small = {str(object_id): data_plans[str(object_id)]}

        hashes_new.extend(links_to_hashes(links_small))
        hash_plans_new.extend(links_to_hashes(link_plans_small))

    # Write all hashes and plans back to the files once
    with open(r'..\hash\hash.txt', 'w') as f:
        f.write("\n".join(hashes_new))

    with open(r'..\hash\hash_plans.txt', 'w') as f:
        f.write("\n".join(hash_plans_new))


def update_hashes(object_id):
    hashes = open_hashes('hash')
    hash_plans = open_hashes('hash_plans')
    
    id = str(object_id).zfill(4)
    
    data = open_json('objects')
    data_plans = open_json('object_plans')

    hashes_new = [x for x in hashes if not x.startswith(id)]
    hash_plans_new = [x for x in hash_plans if not x.startswith(id)]


    links = data[str(object_id)]
    link_plans = data_plans[str(object_id)]

    hashes_new.extend([process_image(object_id, x) for x in links])
    hash_plans_new.extend([process_image(object_id, x) for x in link_plans])

    with open(fr'..\hash\hash.txt', 'w') as f:
        f.write("\n".join(hashes_new))

    with open(fr'..\hash\hash_plans.txt', 'w') as f:
        f.write("\n".join(hash_plans_new))


def check_new():
    ids = parse_last()

    if len(ids) == 0:
        return []

    hashes = open_hashes('hash')
    hash_plans = open_hashes('hash_plans')

    duplicates = []


    for id_ in ids:
        duplicates.extend(compare_object(id_, hashes, skipped_pairs=SKIPPED_PAIRS))
        duplicates.extend(compare_object(id_, hash_plans, skipped_pairs=SKIPPED_PAIRS))

    return duplicates


def links_to_hashes(data_json, name=''):
    strings = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for key, value in data_json.items():
            # if int(key) > 10000:
            #     continue
            for link in value:
                futures.append(executor.submit(process_image, key, link))

        for future in tqdm(as_completed(futures), total=len(futures), leave=False):
            result = future.result()
            if result:
                strings.append(result)

    return strings


def api_parsing():
    num = 1
    url = f'{API_URL}/objects?page={num}&limit=100'
    response = requests.get(url)
    data = response.json()
    json_dict = dict()
    plans_json_dict = dict()
    for item in data['data']:
        photos = []
        plans = []
        for item1 in item['blocks']:
            for item2 in item1['pics']:
                link = item2['url']
                link.replace(' ', '')
                if not item2["isPlan"]:
                    photos.append(link)
                else:
                    plans.append(link)
        for item1 in item['pics']:
            link = item1['url']
            link.replace(' ', '')
            photos.append(link)
        json_dict[item['id']] = photos
        plans_json_dict[item['id']] = plans
    end = int(data['pageCount'])

    for i in tqdm(range(2, end + 1), leave=True):
        url = f'{API_URL}/objects?page={i}&limit=100'
        response = requests.get(url)
        data = response.json()

        for item in data['data']:
            photos = []
            plans = []
            for item1 in item['blocks']:
                for item2 in item1['pics']:
                    link = item2['url']
                    link.replace(' ', '')
                    if not item2["isPlan"]:
                        photos.append(link)
                    else:
                        plans.append(link)
            for item1 in item['pics']:
                link = item1['url']
                link.replace(' ', '')
                photos.append(link)
            json_dict[item['id']] = photos
            plans_json_dict[item['id']] = plans

    data_sorted = collections.OrderedDict(sorted(json_dict.items()))
    data_plans_sorted = collections.OrderedDict(sorted(plans_json_dict.items()))
    
    objects = open_json('objects')
    object_plans = open_json('object_plans')
    
    recheck = set()
    
    for key_ in data_sorted.keys():
        try:
            if len(data_sorted[key_]) != len(objects[str(key_)]):
                recheck.add(key_)
        except:
            recheck.add(key_)
        try:    
            if len(data_plans_sorted[key_]) != len(object_plans[str(key_)]):
                recheck.add(key_)
        except:
            recheck.add(key_)
    
    with open(r'..\db\objects.json', 'w') as fp:
        json.dump(data_sorted, fp)

    with open(r'..\db\object_plans.json', 'w') as fp:
        json.dump(data_plans_sorted, fp)
    
    return recheck


def open_hashes(name):
    with open(fr'..\hash\{name}.txt', 'r') as my_file:
        data = my_file.read()

    hashes = [line for line in data.split('\n') if line]
    print('opened hashes')
    return hashes


def open_json(name):
    with open(fr'..\db\{name}.json', 'r') as my_file:
        data = json.load(my_file)

    return data


def remove_duplicate_lines_in_place(larger_file_path, smaller_file_path):
    # Чтение всех строк из меньшего файла и добавление их в множество для быстрой проверки
    with open(smaller_file_path, 'r', encoding='utf-8') as file:
        smaller_file_lines = set(line.strip() for line in file)

    # Чтение и фильтрация строк из большего файла
    with open(larger_file_path, 'r', encoding='utf-8') as file:
        lines_to_keep = [line for line in file if line.strip() not in smaller_file_lines]

    # Перезапись большего файла только строками, которые должны остаться
    with open(larger_file_path, 'w', encoding='utf-8') as file:
        file.writelines(lines_to_keep)


if __name__ == "__main__":
    api_parsing()