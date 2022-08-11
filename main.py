import logging
import io
import sys
import tqdm
import hashlib
from modules import google, oauth, yandex, vk, db


LOG_FILE_PATH = '.vk_archive_log.txt'
# LS - LOG_STRING
LS_FILE_EXIST = '[{}] File \'{}\' not uploaded, file exists with same checksum: {}'
LS_FILE_UPLOADED = '[{}] File \'{}\' uploaded to the {} folder, photo size: {}'
LS_NOT_ENOUGH_DISK_SPACE = '[{}] There is not enough drive space, the upload is stopped for this drive!'
LS_UPLOAD_FAILED = '[{}] file {} uploading error! {} | Attempts left: {}'
FILE_LIST_FILE_PATH = '.vk_archive_files.json'


def input_or_default(title, default):
    input_data = input(title)
    return default if input_data.strip() == '' else input_data


def checksum(buffer: io.BufferedReader):
    cursor = buffer.tell()
    result = hashlib.md5(buffer.read()).hexdigest()
    buffer.seek(cursor)
    return result


if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILE_PATH, filemode='a',
                        format=u'%(asctime)s,%(msecs)d [%(levelname)s] %(message)s',
                        datefmt='%H:%M:%S', level=logging.INFO)
    config = db.Config('config.yaml')
    vk_token = oauth.VKToken.from_dict(config.get('vk_token'))
    if vk_token is None or not vk_token.is_valid():
        user_input = input('Потребуется получить токен VK, для этого нужно открыть браузер, Вы готовы? (Да/Нет): ')
        if not user_input.lower().startswith('д'):
            print('Невозможно продоложить без токена VK, программа завершена')
            sys.exit()
        vk_token = oauth.receive_token_vk()
        if vk_token:
            config.set('vk_token', vk_token.to_dict())
        else:
            print('Произошла ошибка при получении токена. Невозможно продоложить без токена VK, программа завершена')
            sys.exit()
    vk_main = vk.VK(vk.VKAPI(vk_token))
    vk_user = vk_main.user(input('Введите ID пользователя в VK: '))
    if vk_user.deactivated or not vk_user.can_access_closed:
        'Пользователь удалил аккаунт или закрыл к нему доступ. Программа завершена.'
        sys.exit()
    user_input = int(input_or_default(
        'Куда потребуется сохранить фото?\n'
        '1. Яндекс диск\n'
        '2. Google диск\n'
        '3. Оба варианта\n'
        'Введите цифру (по умолчанию 1): ', 1
    ))
    yadisk = user_input in [1, 3]
    gdrive = user_input in [2, 3]
    if yadisk and not config.get('yandex_token'):
        config.set('yandex_token', input('Введите токен Yandex:\n'))
    google_token = oauth.GoogleToken.from_dict(config.get('google_token'))
    if gdrive and not google_token or not google_token.is_valid():
        user_input = input('Потребуется получить токен Google, для этого нужно открыть браузер, Вы готовы? (Да/Нет): ')
        if not user_input.lower().startswith('д'):
            gdrive = False
            config.set('google_token', None)
        else:
            google_token = oauth.receive_token_google()
            if not google_token:
                gdrive = False
                config.set('google_token', None)
            else:
                config.set('google_token', google_token.to_dict())
    if not yadisk and not gdrive:
        print("Место хранения фотографий не задано, программа завершена")
        sys.exit()
    gdrive_path = config.get('google_folder') or input_or_default(
        'Введите путь к папке хранения на диске google (по умолчанию /_vk_photo_archive/): ',
        '/_vk_photo_archive/'
    ) if gdrive else None
    yandex_path = config.get('yandex_folder') or input_or_default(
        'Введите путь к папке хранения на диске yandex (по умолчанию /_vk_photo_archive/): ',
        '/_vk_photo_archive/'
    ) if yadisk else None
    print('Инициализция дисков...')
    google_folder_files_list = {}
    if gdrive_path:
        gdrive = google.GoogleDrive(google.GoogleDriveAPIv3(google_token))
        if not gdrive.resolve_path(gdrive_path):
            gdrive.create_folder(gdrive_path, create_parent_folders=True)
        gdrive = gdrive.resolve_path(gdrive_path)[-1]
        config.set('google_folder', gdrive_path)
        data = gdrive.api.files_list(fields=['files:md5Checksum', 'files:name'])
        if 'files' in data and len(data['files']):
            for item in data['files']:
                if 'md5Checksum' in item:
                    google_folder_files_list[item['md5Checksum']] = item['name']
    yandex_folder_files_list = {}
    if yandex_path:
        yadisk = yandex.YaDisk(config.get('yandex_token'))
        if not yadisk.resolve_path(yandex_path):
            yadisk.create_folders(yandex_path)
        config.set('yandex_folder', yandex_path)
        for item in yadisk.files_list(yandex_path, 'image', 'items.md5'):
            yandex_folder_files_list[item['md5']] = item['name']
    photo_count = int(input('Сколько фотографий нужно загрузить? Введите цифру (0 - все): '))
    use_albums = input('Сохранить фотографии из альбомов? (Да/Нет): ').lower().startswith('д')
    print(f'Подгружаю данные о фотографиях...')
    photos = vk_user.get_photos(count=photo_count)
    if use_albums:
        albums = vk_user.photo_albums()
        photos += vk_user.get_photos_from_albums(albums, photo_count=photo_count)
    files_list = db.FileList()
    bar_format = '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed} | {remaining}, {rate_fmt}{postfix}]'
    print(f'Подробную информацию процесса загрузки фото можно посмотреть в логе: {LOG_FILE_PATH}')
    for photo in tqdm.tqdm(photos, colour='green', desc='Загрузка фото на диск(и)', unit='фото', bar_format=bar_format):
        size = photo.max_size
        mime_type = size.mime_type
        file_ext = size.file_extension
        file_name = f'{photo.likes_count}.{file_ext}'
        image_data = size.read_bytes()
        md5 = checksum(image_data)
        exists_on_google = None
        exists_on_yandex = None
        i = 0
        while True:
            if gdrive and any([True for item in google_folder_files_list.values() if file_name == item]):
                if not google_folder_files_list.get(md5):
                    i = len([
                        True for item in google_folder_files_list.values() if item.startswith(f'{photo.likes_count}_')
                    ])
                    file_name = f'{photo.likes_count}_{str(photo.date.date())}.{file_ext}'
                    if i:
                        file_name = file_name.replace('.', f'({i}).')
                    i += 1
                    continue
            if yadisk and any([True for item in yandex_folder_files_list.values() if file_name == item]):
                if not yandex_folder_files_list.get(md5):
                    i = len([
                        True for item in yandex_folder_files_list.values() if item.startswith(f'{photo.likes_count}_')
                    ])
                    file_name = f'{photo.likes_count}_{str(photo.date.date())}.{file_ext}'
                    if i:
                        file_name = file_name.replace('.', f'({i}).')
                    i += 1
                    continue
            break
        for retry in range(0, 3):
            try:
                if isinstance(gdrive, google.GoogleDriveItem):
                    exist_file_name = google_folder_files_list.get(md5)
                    if exist_file_name:
                        logging.info(LS_FILE_EXIST.format('GoogleDrive', file_name, exist_file_name))
                        break
                    elif gdrive.upload_file(image_data, mime_type, file_name):
                        logging.info(LS_FILE_UPLOADED.format('GoogleDrive', file_name, gdrive_path, str(size).upper()))
                        google_folder_files_list[md5] = file_name
                        files_list.add(file_name, str(size), md5, f'google:/{gdrive_path}')
                        break
            except google.StorageQuotaExceeded as error:
                logging.error(LS_NOT_ENOUGH_DISK_SPACE.format('GoogleDrive'))
                gdrive = None
            except db.FileAlreadyExistInFileList as error:
                logging.warning(f'File \'{file_name}\' already in the list: {FILE_LIST_FILE_PATH}. ')
            except Exception as error:
                logging.error(LS_UPLOAD_FAILED.format('GoogleDrive', file_name, str(error), 2 - retry))
        for retry in range(0, 3):
            try:
                if yadisk:
                    exist_file_name = yandex_folder_files_list.get(md5)
                    if exist_file_name:
                        logging.info(LS_FILE_EXIST.format('YandexDisk', file_name, exist_file_name))
                        break
                    elif yadisk.upload(f'{yandex_path}/{file_name}', image_data, mime_type, max_retry_count=1):
                        logging.info(LS_FILE_UPLOADED.format('YandexDisk', file_name, gdrive_path, str(size).upper()))
                        yandex_folder_files_list[md5] = file_name
                        files_list.add(file_name, str(size), md5, f'yandex:/{yandex_path}')
                        break
            except yandex.InsufficientStorageException as error:
                logging.error(LS_NOT_ENOUGH_DISK_SPACE.format('YandexDisk'))
                yadisk = None
            except db.FileAlreadyExistInFileList as error:
                logging.warning(f'File already in the list {FILE_LIST_FILE_PATH}. ')
            except Exception as error:
                logging.error(LS_UPLOAD_FAILED.format('YandexDisk', file_name, str(error), 2 - retry))
    print(f'Загрузка фотографий завершена. Результаты загрузки в файле {FILE_LIST_FILE_PATH}')
    files_list.save_to_file(FILE_LIST_FILE_PATH)
