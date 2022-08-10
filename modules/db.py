import json
import logging
import os
import yaml


class FileAlreadyExistInFileList(Exception):
    pass


class Config:
    def __init__(self, path):
        self._params = {
            'vk_token': None,
            'google_token': None,
            'yandex_token': None,
            'google_folder': None,
            'yandex_folder': None
        }
        self.path = path
        if os.path.isfile(path):
            with open(path) as file:
                data = yaml.safe_load(file)
                for param in self._params.keys():
                    if param in data:
                        self._params[param] = data[param]

    def get(self, param):
        return self._params[param] if param in self._params else None

    def set(self, param, value):
        if param not in self._params:
            raise Exception(f'Unknown parameter: {param}')
        old_value = self._params.get(param)
        self._params[param] = value
        if old_value != value:
            with open(self.path, 'w') as file:
                yaml.dump(self._params, file)


class FileList:
    def __init__(self, path: str = None):
        self._items = []
        self._path = path
        if path and os.path.isfile(path):
            with open(path) as file:
                try:
                    data = json.load(file)
                    self._items = list(data)
                except Exception as error:
                    logging.error(f'Failed to parse files list: {error}')
                    pass

    def get(self, name_or_checksum: str):
        for item in self._items:
            if item['file_name'] == name_or_checksum or item['checksum'] == name_or_checksum:
                return item
        return None

    def add(self, name: str, size: str, checksum: str, disk: str):
        if item := (self.get(name) or self.get(checksum)):
            if disk not in item['disks']:
                item['disks'].append(disk)
                return
            raise FileAlreadyExistInFileList('Information about this file already exists!')
        self._items.append({
            'file_name': name,
            'size': size,
            'checksum': checksum,
            'disks': [disk]
        })

    def save_to_file(self, path: str = None):
        file_path = path or self._path
        with open(file_path, 'w') as file:
            json.dump(self._items, file)
