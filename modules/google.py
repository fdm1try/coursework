import io
import requests
from modules.oauth import GoogleToken


class StorageQuotaExceeded(Exception):
    pass


class GoogleDriveUnknownError(Exception):
    def __init__(self, code: int, error_info, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.error = {'code': code, 'info': error_info}


GOOGLE_FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'


def parse_fields(fields: list):
    if not fields:
        return None
    result = {}
    for field in fields:
        cursor = result
        for part in field.split(':'):
            if part not in cursor:
                cursor[part] = {}
            cursor = cursor[part]

    def pack(options: dict):
        return ','.join([
            key if not len(value) else f'{key}({pack(value)})'
            for key, value in options.items()
        ])
    return pack(result)


class GoogleDriveAPI:
    version = None

    def __init__(self, oauth_token: GoogleToken):
        self.oauth = oauth_token

    @property
    def _auth_header(self):
        return {'Authorization': f'Bearer {self.oauth.access_token}'}

    def _is_error(self, response: dict, silent=False):
        if 'error' in response:
            code = response['error']['code']
            message = response['error']['message']
            reason = response['error']['errors'][0]['reason']
            if silent:
                return code
            if code == 404:
                raise FileNotFoundError(message)
            elif code == 403 and reason == 'storageQuotaExceeded':
                raise StorageQuotaExceeded(message)
            raise GoogleDriveUnknownError(code, response['error']['errors'][0])

    @staticmethod
    def parse_fields(fields: list):
        if not fields:
            return None
        result = {}
        for field in fields:
            cursor = result
            for part in field.split(':'):
                if part not in cursor:
                    cursor[part] = {}
                cursor = cursor[part]

        def pack(options: dict):
            return ','.join([
                key if not len(value) else f'{key}({pack(value)})'
                for key, value in options.items()
            ])

        return pack(result)

    def file_create(self, name: str, **params):
        pass

    def files_list(self, search_query=None, fields: list = []):
        pass

    def file_get_info(self, file_id, *fields):
        pass

    def file_update_data(self, file_id, data):
        pass

    def file_update_meta(self, file_id, **metadata):
        pass

    def file_delete(self, file_id):
        pass


class GoogleDriveAPIv3(GoogleDriveAPI):
    version = 3

    def files_list(self, search_query=None, fields: list = []):
        url = f'https://www.googleapis.com/drive/v{self.version}/files'
        fields = GoogleDriveAPI.parse_fields(
            fields + ['nextPageToken', 'files:id', 'files:name', 'files:mimeType']
        )
        params = {'fields': fields}
        if search_query:
            params['q'] = search_query
        result = None
        while True:
            response = requests.get(url=url, headers=self._auth_header, params=params)
            data = response.json()
            if self._is_error(data, silent=True) == 404:
                return None
            self._is_error(data)
            if result is None:
                result = data
            elif 'files' in data and len(data['files']):
                result['files'] += data['files']
            if 'nextPageToken' in data and data['nextPageToken']:
                params['pageToken'] = data['nextPageToken']
            else:
                break
        return result

    def file_get_info(self, file_id, *fields):
        url = f'https://www.googleapis.com/drive/v3/files/{file_id}'
        params = {'fields': ','.join(fields)} if fields else None
        response = requests.get(url=url, headers=self._auth_header, params=params)
        data = response.json()
        self._is_error(data)
        return data

    def file_create(self, name: str, **params):
        url = 'https://www.googleapis.com/drive/v3/files'
        params = params or {}
        response = requests.post(url=url, headers=self._auth_header, json={
            'name': name,
            **params
        })
        data = response.json()
        self._is_error(data)
        return data

    def file_update_data(self, file_id, buffer: io.BufferedReader):
        cursor = buffer.tell()
        url = f'https://www.googleapis.com/upload/drive/v3/files/{file_id}'
        headers = {'Content-type': 'application/octet-stream', **self._auth_header}
        response = requests.patch(url=url, headers=headers, data=buffer, params={'uploadType': 'media'})
        buffer.seek(cursor)
        data = response.json()
        self._is_error(data)
        return True

    def file_update_meta(self, file_id, **metadata):
        url = f'https://www.googleapis.com/drive/v3/files/{file_id}'
        response = requests.patch(url=url, headers=self._auth_header, json=metadata)
        data = response.json()
        self._is_error(data)
        return data

    def file_delete(self, file_id):
        url = f'https://www.googleapis.com/drive/v3/files/{file_id}'
        response = requests.delete(url=url, headers=self._auth_header)
        if len(response.text):
            self._is_error(response.json())
        else:
            return True


class GoogleDriveItem:
    def __init__(self, api: GoogleDriveAPI, item_id: str, name: str, mime_type: str):
        self.api = api
        self._id = item_id
        self._name = name
        self._mime_type = mime_type

    @staticmethod
    def from_dict(api: GoogleDriveAPI, params: dict):
        """
        :param api: GoogleDriveAPI instance
        :param params: dictionary, required params: id, name, mimeType
        :return: GoogleDriveItem instance
        """
        kwargs = {}
        for arg, param in [('item_id', 'id'), ('name', 'name'), ('mime_type', 'mimeType')]:
            if param not in params:
                raise Exception(f'{param} is required!')
            kwargs[arg] = params[param]
        return GoogleDriveItem(api, **kwargs)

    @staticmethod
    def load(api: GoogleDriveAPI, item_id: str):
        """
        :param api: GoogleDriveAPI instance
        :param item_id: object id in google drive
        :return: GoogleDriveItem instance
        """
        return GoogleDriveItem.from_dict(api, api.file_get_info(item_id, 'id', 'name', 'mimeType'))

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def mime_type(self) -> str:
        return self._mime_type

    @property
    def size(self):
        r = 0 if self.is_folder() else int(self.get_info('size'))
        return r

    @property
    def md5(self) -> str:
        return self.get_info('md5Checksum')

    @property
    def root_folder(self):
        return GoogleDriveItem.from_dict(self.api, self.api.file_get_info('root', 'id', 'name', 'mimeType'))

    def is_folder(self) -> bool:
        """
        :return: True if this is a folder
        """
        return self.mime_type == GOOGLE_FOLDER_MIME_TYPE

    def is_file(self) -> bool:
        """
        :return: rue if this is a file
        """
        return not self.is_folder()

    def parents(self) -> list:
        """
        :return: list of GoogleDriveItem instances that are parents
        """
        if isinstance(self, GoogleDrive):
            return []
        parents = self.api.file_get_info(self.id, 'parents')
        return [
            GoogleDriveItem.load(self.api, parent_id)
            for parent_id in parents
        ]

    def items(self, **filters) -> list:
        """
        :param filters: filters in format field=value, for example name='file_name.ext'
        :return: list of GoogleDriveItem instances - found child items
        """
        search_query = ' and '.join([f'{key} = "{value}"' for key, value in filters.items()])
        search_query += f' and "{self.id}" in parents and trashed = false'
        data = self.api.files_list(search_query=search_query)
        if data and 'files' in data and len(data['files']):
            return list([GoogleDriveItem.from_dict(self.api, item) for item in data['files']])

    def get_info(self, *fields):
        """
        :param fields: list of fields to get
        :return: if there is only one element in the fields list, a string will be returned, otherwise a dictionary
        """
        data = self.api.file_get_info(self.id, *fields)
        return data if len(fields) > 1 else data[fields[0]]

    def folders(self, **filters):
        """
        :param filters: filters in format field=value, for example name='file_name.ext'
        :return: list of GoogleDriveItem instances - found child folders
        """
        filters = filters or {}
        filters['mimeType'] = GOOGLE_FOLDER_MIME_TYPE
        return self.items(**filters)

    def files(self, **filters):
        """
        :param filters: filters in format field=value, for example name='file_name.ext'
        :return: list of GoogleDriveItem instances - found child files
        """
        search_query = '"{self.id}" in parents and mimeType != "application/vnd.google-apps.folder"'
        if filters:
            search_query += ' and ' ' and '.join([f'{key} = "{value}"' for key, value in filters.items()])
        data = self.api.files_list(search_query=search_query)
        if data and 'files' in data and len(data['files']):
            return list([
                GoogleDriveItem(self.api, item['id'], item['name'], item['mimeType'])
                for item in data['files']
            ])

    def resolve_path(self, remote_path: str = '/'):
        """
        :param remote_path: path in format /folder/file.ext
        :return: tuple of GoogleDriveItem instances if the path exists
        """
        path_parts = list(filter(lambda x: len(x), remote_path.split('/')))
        q = ' or '.join(list(map(lambda x: f'name = "{x}"', path_parts[0:-1])))
        q = (
            f'name="{path_parts[-1]}" or (mimeType="{GOOGLE_FOLDER_MIME_TYPE}" and ({q}))'
            if len(path_parts) > 1
            else f'name="{path_parts[-1]}"'
        )
        data = self.api.files_list(search_query=q, fields=['files:parents'])
        if remote_path.startswith('/'):
            root_item = self.api.file_get_info('root', 'name', 'id', 'mimeType')
            data['files'].append(root_item)
            path_parts = [root_item.get('name')] + path_parts
        name_list = set(map(lambda item: item['name'], data['files']))
        if not data or any(map(lambda name: name not in name_list, path_parts)):
            return None

        def resolve_next(items: list, resolved_path: list = []):
            if not items or not len(items):
                return resolved_path if len(resolved_path) else None
            for item in [item for item in data['files'] if item['name'] == items[-1] and (
                item['id'] in resolved_path[0]['parents'] if len(resolved_path) else True
            )]:
                if result := resolve_next(items[0:-1], [item] + resolved_path):
                    return result
            return None

        if gdrive_path := resolve_next(path_parts):
            return tuple([GoogleDriveItem.from_dict(self.api, item) for item in gdrive_path])
        return None

    def create_folder(self, path, create_parent_folders: bool = False):
        """
        :param path: path in format /parent_folder/folder
        :param create_parent_folders: create parent folders if there are none
        :return: GoogleDriveItem instance - folder that was created
        """
        parts = path.strip('/').split('/')
        prev = self.root_folder if path.startswith('/') else self
        for i, part in enumerate(parts):
            items = prev.items(name=part)
            current = items[0] if items else None
            if current:
                prev = current
            elif i + 1 == len(parts):
                data = self.api.file_create(part, **{'mimeType': GOOGLE_FOLDER_MIME_TYPE, 'parents': [self.id]})
                return GoogleDriveItem.from_dict(self.api, data)
            elif create_parent_folders:
                data = self.api.file_create(part, **{'mimeType': GOOGLE_FOLDER_MIME_TYPE, 'parents': [self.id]})
                current = GoogleDriveItem.from_dict(self.api, data)
            else:
                return None
            prev = current
        return prev

    def create_file(self, path: str, mime_type: str, overwrite=False, create_parent_folders=False, **meta_attrs):
        """
        :param path: path in format /folder/file_name.ext
        :param mime_type: mime type of file
        :param overwrite: overwrite if True
        :param create_parent_folders: create parent folders if there are none
        :param meta_attrs: add file meta attrs
        :return: GoogleDriveItem instance - file that was created
        """
        gdrive_path = self.resolve_path(path)
        if gdrive_path:
            if not overwrite:
                raise Exception('File already exists!')
            file = gdrive_path[-1]
            return file.update_meta(**{**meta_attrs, 'mimeType': mime_type})
        # this path should be resolved: /file.ext
        if path.rfind('/') < 2:
            data = self.api.file_create(path, **{**meta_attrs, 'mimeType': mime_type, 'parents': [self.id]})
            return GoogleDriveItem.from_dict(self.api, data)
        parts = list(filter(lambda x: len(x), path.split('/')))
        file_name = parts.pop()
        folder_path = '/'.join(parts)
        if folder := self.create_folder(f'{"/" if path.startswith("/") else ""}{folder_path}', create_parent_folders):
            data = self.api.file_create(file_name, **{**meta_attrs, 'mimeType': mime_type, 'parents': [folder.id]})
            return GoogleDriveItem.from_dict(self.api, data)

    def update_meta(self, **metadata):
        """
        :param metadata: file metadata to be changed
        :return: self, GoogleDriveItem instance that was changed
        """
        if not self.is_file():
            raise Exception('Updating metadata is only possible in files.')
        data = self.api.file_update_meta(self.id, **metadata)
        if name := data.get('name'):
            self._name = name
        if mime_type := data.get('mimeType'):
            self._mime_type = mime_type
        return self

    def upload_file(self, data: io.BufferedReader, mime_type='application/content-stream',
                    path=None, overwrite=False, create_parent_folders=False, **meta_attrs):
        """
        :param data: data buffer to be uploaded
        :param mime_type: mime type of the file
        :param path: update the data of the file (current instance) if the path is not specified
        :param overwrite: overwrite if True
        :param create_parent_folders: create parent folders if there are none
        :param meta_attrs: file metadata to be changed or created
        :return: file that was uploaded
        """
        if self.is_file() and path is not None:
            raise Exception('Can not create file inside other file. Call this method from folder.')
        gdrive_path = self.resolve_path(path) if path else (self, )
        if gdrive_path and not overwrite:
            if gdrive_path[-1].size:
                raise Exception('File already exists')
        file = (
            gdrive_path[-1] if gdrive_path
            else self.create_file(path, mime_type, overwrite, create_parent_folders, **meta_attrs)
        )
        if self.api.file_update_data(file.id, data):
            return file

    def delete(self) -> bool:
        """
        :return: True if deleted
        """
        result = self.api.file_delete(self.id)
        if not result:
            return False
        self._id = None
        self._name = None
        self._mime_type = None
        return True

    def __str__(self):
        return self._name

    def __repr__(self):
        return f'{self._name} ({self._mime_type if self._mime_type != GOOGLE_FOLDER_MIME_TYPE else "FOLDER"})'

    def __eq__(self, other):
        return isinstance(other, GoogleDriveItem) and self.id and self.id == other.id and self.name == other.name


class GoogleDrive(GoogleDriveItem):
    def __init__(self, api: GoogleDriveAPI):
        super().__init__(api, 'root', '/', GOOGLE_FOLDER_MIME_TYPE)
