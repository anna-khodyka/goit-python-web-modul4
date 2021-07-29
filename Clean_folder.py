import pathlib
import sys
import shutil
import os


def translate(string):
    '''транслетирирует буквы кирилицы в латиницу'''
    CYRILLIC_SYMBOLS = "абвгдеёжзийклмнопрстуфхцчшщъыьэюяєіїґ"
    TRANSLATION = ("a", "b", "v", "g", "d", "e", "e", "j", "z", "i", "j", "k", "l", "m", "n", "o", "p", "r", "s", "t", "u",
               "f", "h", "ts", "ch", "sh", "sch", "", "y", "", "e", "yu", "u", "ja", "je", "ji", "g")

    TRANS = {}
    for c, t in zip(CYRILLIC_SYMBOLS, TRANSLATION):    
        TRANS[ord(c)]= t
        TRANS[ord(c.upper())] = t.upper()
    
    translated_string = string.translate(TRANS)

    return translated_string


def normalize(string):
    '''функция не только транслетерирует но и заменяет небуквы и нецифры на _ '''
    import re
    normalized_string = translate(string)
    normalized_string = re.sub(r'[^0-9a-zA-Z ]', '_', normalized_string, flags=re.ASCII)

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
            normalized_file_name = normalized_file_name  + normalize(part) + '.'
        else:
            normalized_file_name = normalized_file_name + normalize(part) + '_'
    return normalized_file_name


def parse_recursion(folder_path, files_dict):
    
    EXTENTION_DICT = {'images': {'.jpeg', '.png', '.jpg', '.svg'},
                      'video': {'.avi', '.mp4', '.mov', '.mkv'},
                      'documents': {'.doc', '.docx', '.txt', '.pdf', '.xlsx', '.pptx'},
                      'audio': {'.mp3', '.ogg', '.wav', '.amr'},
                      'archives': {'.zip', '.gz', '.tar'}}
    
    # перебор всех объектов в папке и их анализ  
    for object in folder_path.iterdir():
        
        # если объект в папке - файл
        if object.is_file():
            
            flag = False    
            
            for file_type, file_ext_set in EXTENTION_DICT.items():
                if object.suffix.lower() in file_ext_set:
                    if files_dict.get(file_type) == None:
                        files_dict[file_type] = list()
                    files_dict[file_type].append(object.absolute())
                    flag = True

            # если расширение файла не соотв не одному типу файлов со справочника
            if not flag:
                if files_dict.get('others') == None:
                    files_dict['others'] = list()
                files_dict['others'].append(object.absolute())

        # если объект в папке - папка, вызываем рекурсию
        if object.is_dir():
            parse_recursion(object, files_dict)

    return files_dict       


def move_files(root_folder, files_dict):
    # перемещаем файлы в подпапки в зависимости от типа файла
    for file_type, files_list in files_dict.items():
        for file in files_list:
            # проверяем есть ли подпапка в названием типа файла, если нет - создаем ее
            if not (root_folder/ file_type).exists():
                os.makedirs(root_folder/ file_type)
            
            # перемещаем файл в нужную подпапку 
            if file_type!='archives':
                #если файл не архив
                normalized_file_name = normalize_file_name(file.name)
                destination_path = root_folder/ file_type / normalized_file_name
                shutil.move(file, destination_path)
            else:
            # перемещаем файл в нужную подпапку - если файл - архив
                new_folder_for_archive = root_folder/ file_type / normalize(file.name.removesuffix(file.suffix))
                os.makedirs(new_folder_for_archive)
                shutil.unpack_archive(file, new_folder_for_archive)
                os.remove(file)


def remove_empty_folders (folder_path):
    '''удаляет все пустые подпапки в папке folder_path '''

    for object in folder_path.iterdir():
        if object.is_dir():
            if not os.listdir(object):
                os.removedirs(object)
            else:
                remove_empty_folders (object)  


def parse_folder(folder_path):
    '''функция разбирает папку согласно задания из ДЗ'''

    # проверка существует ли папка
    if folder_path.exists() == False:
        print(f'Указанная папка с путем {folder_path.absolute()} не существует')
        return
    if folder_path.exists() and (folder_path.is_dir() == False):
        print(f'Указанный путь {folder_path.absolute()} - не папка')
        return
    # составляем каталог файлов, где ключ словаря - тип файла
    files_dict = dict()
    files_dict = parse_recursion(folder_path, files_dict)
    print(files_dict)
    # перемещаем файлы в подпапки в зависимости от типа файла
    move_files(folder_path, files_dict)
    # удаляет все пустые подпапки 
    remove_empty_folders (folder_path)


def main():
    if len(sys.argv) == 1:
        print('Путь к папке не введен')
        return

    folder_path = sys.argv[1]
    folder_path = pathlib.Path(folder_path)
    parse_folder(folder_path)

if __name__ == '__main__':
    main()