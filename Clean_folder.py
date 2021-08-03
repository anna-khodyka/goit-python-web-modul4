'''

Напишите программу обработки папки "Хлам",
которая сортирует файлы в указанной папке по расширениям с использованием нескольких потоков.
Ускорьте обработку больших каталогов с большим количеством вложенных папок и файлов
за счет параллельного выполнения обхода всех папок в отдельных потоках.

Наиболее затратным по времени будет:
 перенос файла и
 получение списка файлов в папке(итерация по содержимому каталога).

 Чтобы ускорить перенос файлов его можно выполнять в отдельном потоке или пуле потоков.
 Это тем более удобно, что результат этой операции вы в приложении не обрабатываете
 и можно не собирать никакие результаты.

 Чтобы ускорить обход содержимого каталога с несколькими уровнями вложенности
 вы можете обработку каждого подкаталога выполнять в отдельном потоке
 или передавать обработку в пул потоков.
'''

import pathlib
import sys
import shutil
import os
import re
import time

from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Semaphore

temp_folder = pathlib.Path('d:\Hlam1')


def translate(string):
    '''транслетирирует буквы кирилицы в латиницу'''
    CYRILLIC_SYMBOLS = "абвгдеёжзийклмнопрстуфхцчшщъыьэюяєіїґ"
    TRANSLATION = ("a", "b", "v", "g", "d", "e", "e", "j", "z", "i", "j", "k", "l", "m", "n", "o", "p", "r", "s", "t", "u",
                   "f", "h", "ts", "ch", "sh", "sch", "", "y", "", "e", "yu", "u", "ja", "je", "ji", "g")

    TRANS = {}
    for c, t in zip(CYRILLIC_SYMBOLS, TRANSLATION):
        TRANS[ord(c)] = t
        TRANS[ord(c.upper())] = t.upper()

    translated_string = string.translate(TRANS)

    return translated_string


def normalize(string):
    '''функция не только транслетерирует но и заменяет небуквы и нецифры на _ '''
    normalized_string = translate(string)
    normalized_string = re.sub(
        r'[^0-9a-zA-Z ]', '_', normalized_string, flags=re.ASCII)

    return normalized_string


def normalize_file_name(file_name):
    ''' функция нормализует название файла при этом не трогает расширение файла'''
    counter = 0
    normalized_file_name = ''
    for part in file_name.split('.'):
        counter += 1
        if counter == len(file_name.split('.')):
            normalized_file_name += normalize(part)
        elif counter == len(file_name.split('.'))-1:
            normalized_file_name = normalized_file_name + normalize(part) + '.'
        else:
            normalized_file_name = normalized_file_name + normalize(part) + '_'
    return normalized_file_name


def parse_recursion(folder_path, files_dict):
    # функция обходит содержимое папки, если есть подпапка - рекурсивно вызывает себя
    EXTENTION_DICT = {'images': {'.jpeg', '.png', '.jpg', '.svg'},
                      'video': {'.avi', '.mp4', '.mov', '.mkv'},
                      'documents': {'.doc', '.docx', '.txt', '.pdf', '.xlsx', '.pptx', '.xml'},
                      'audio': {'.mp3', '.ogg', '.wav', '.amr'},
                      'archives': {'.zip', '.gz', '.tar'}}
    # перебор всех объектов в папке и их анализ
    for object in folder_path.iterdir():

        # если объект в папке - файл
        # заносим путь+название файла в files_dict
        if object.is_file():
            # определяем тип файла
            file_type = None
            for f_type, file_ext_set in EXTENTION_DICT.items():
                if object.suffix.lower() in file_ext_set:
                    file_type = f_type
            if not file_type:
                file_type = 'others'

            # если такой тип файлов попался первый раз
            # создаем в словаре новую запись
            if files_dict.get(file_type) == None:
                files_dict[file_type] = list()

            # делаем запись в словарь
            files_dict[file_type].append(object.absolute())

        # если объект в папке - папка, вызываем рекурсию в отдельном потоке
        if object.is_dir():
            t = Thread(target=parse_recursion, args=(object, files_dict))
            t.start()
            t.join()
            # parse_recursion(object, files_dict)
    return files_dict


def move_file(cond, root_folder, file, file_type):
    # функция перемещает файл в папку с названием типа файла, если архив - разархивирует его
    if not (root_folder / file_type).exists():
        os.makedirs(root_folder / file_type)

    cond.acquire()
    # перемещаем файл в нужную подпапку
    if file_type != 'archives':
        # если файл не архив
        normalized_file_name = normalize_file_name(file.name)
        destination_path = root_folder / file_type / normalized_file_name
        shutil.move(file, destination_path)
    else:
        # перемещаем файл в нужную подпапку - если файл - архив
        new_folder_for_archive = root_folder / file_type / \
            normalize(file.name.removesuffix(file.suffix))
        os.makedirs(new_folder_for_archive)
        shutil.unpack_archive(file, new_folder_for_archive)
        os.remove(file)
    cond.release()


def move_files(folder_path, files_dict):
    # перемещаем файлы в подпапки в зависимости от типа файла

    # синхронный код
    # for file_type, files_list in files_dict.items():
    #     for file in files_list:
    #         move_file(folder_path, file, file_type)

    # Симафор
    pool = Semaphore(2)
    threads = []
    for file_type, files_list in files_dict.items():
        for file in files_list:
            thread = Thread(target=move_file, args=(
                pool, folder_path, file, file_type))
            thread.start()
            threads.append(thread)
    while threads:
        threads.pop().join()

    # Пул потоков
    # with ThreadPoolExecutor(max_workers=2) as pool:
    #     for file_type, files_list in files_dict.items():
    #         for file in files_list:
    #             pool.submit(move_file, folder_path, file, file_type)


def remove_empty_folders(folder_path):
    '''удаляет все пустые подпапки в папке folder_path '''

    for object in folder_path.iterdir():
        if object.is_dir():
            if not os.listdir(object):
                os.removedirs(object)
            else:
                remove_empty_folders(object)


def parse_folder(folder_path):
    '''функция разбирает папку согласно задания из ДЗ'''

    files_dict = dict()

    # создаем словарь с всеми файлами
    files_dict = parse_recursion(folder_path, files_dict)
    # создаем подпапки с названиями типов файлов
    for file_type in files_dict:
        os.makedirs(folder_path / file_type)
    # перемещаем файлы в подпапки в зависимости от типа файла
    move_files(folder_path, files_dict)
    # удаляет все пустые подпапки
    remove_empty_folders(folder_path)


if __name__ == "__main__":

    # if (pathlib.Path('D:\Hlam')).exists():
    #     shutil.rmtree(pathlib.Path('D:\Hlam'))

    for i in range(10):
        # создаем папку для опыта
        folder_path = shutil.copytree('D:\Hlam_template', 'D:\Hlam')
        folder_path = pathlib.Path(folder_path)
        started = time.time()
        parse_folder(folder_path)
        elapsed = time.time() - started
        print(elapsed)
        # удаляем папку для опыта
        shutil.rmtree(folder_path)
