import io
import requests
from os import path


class PayloadTooLargeException(Exception):
    pass


class PreconditionFailedException(Exception):
    pass


class InsufficientStorageException(Exception):
    pass


class YaDisk:
    def __init__(self, token: str):
        self.token = token

    def _is_api_error(self, response_data: dict):
        """
        :param response_data: JSON response data from Yandex API
        :return: False if there are no errors in the response body, otherwise raise exception
        """
        _ = self.token
        if 'error' in response_data:
            raise Exception(
                f'Yandex API {response_data["error"]}: {response_data["description"]}\n' +
                response_data["message"]
            )
        return False

    def _get_upload_link(self, remote_path: str, overwrite=False):
        """
        :param remote_path: the path on yandex disk
        :param overwrite:  do not overwrite by default
        :return:
        """
        endpoint = 'https://cloud-api.yandex.net/v1/disk/resources/upload'
        params = {
            'path': remote_path.replace('//', '/'),
            'overwrite': bool(overwrite)
        }
        headers = {'Authorization': f'OAuth {self.token}'}
        response = requests.get(endpoint, headers=headers, params=params)
        data = response.json()
        if not self._is_api_error(data):
            return data.get('href')

    def create_folders(self, remote_path: str):
        """
        :param remote_path: the path on yandex disk
        :return: True if folders are created
        """
        remote_path = remote_path.replace('//', '/')
        if self.resolve_path(remote_path):
            raise FileExistsError("The specified path already exists")
        path_parts = list(path.split(remote_path))
        current_path = ''
        while path_parts:
            current_path += path_parts.pop(0)
            if not self.resolve_path(current_path):
                if not self.create_folder(current_path):
                    return False
        return True

    def create_folder(self, remote_path):
        """
        :param remote_path: the path on yandex disk
        :return: True if folder are created
        """
        endpoint = 'https://cloud-api.yandex.net/v1/disk/resources'
        headers = {'Authorization': f'OAuth {self.token}'}
        params = {'path': remote_path.replace('//', '/')}
        response = requests.put(endpoint, headers=headers, params=params)
        if response.status_code == 201:
            return True
        self._is_api_error(response.json())
        return False

    def upload(self, remote_path: str, buffer: io.BufferedReader, mime_type: str = 'application/octet-stream',
               overwrite: bool = False, create_parent_folders: bool = False, max_retry_count: int = 3):
        """
        :param remote_path: the path to the file on yandex disk
        :param buffer: the buffer from which the file can be read
        :param mime_type: file mime type
        :param overwrite: do not overwrite by default
        :param create_parent_folders: create directories in the specified path if they do not exist
        :param max_retry_count: number of upload attempts
        :return: None
        """
        remote_path = remote_path.replace('//', '/')
        if create_parent_folders:
            self.create_folders(remote_path)
        endpoint = self._get_upload_link(remote_path, overwrite)
        headers = {
            'Authorization': f'OAuth {self.token}',
            'Content-type': mime_type
        }
        retry_count = 0
        while True:
            response = requests.put(endpoint, headers=headers, data=buffer)
            if response.status_code in [201, 202]:
                return True
            if response.status_code == 412:
                raise PreconditionFailedException(
                    'Yandex API error: Precondition Failed\n'
                    'При дозагрузке файла был передан неверный диапазон в заголовке Content-Range'
                )
            if response.status_code == 413:
                raise PayloadTooLargeException('Yandex API error: Payload Too Large\nРазмер файла больше допустимого.')
            if response.status_code == 507:
                raise InsufficientStorageException(
                    'Yandex API error: Insufficient Storage\nДля загрузки файла не хватает места на Диске.'
                )
            retry_count += 1
            if retry_count == max_retry_count:
                raise Exception(
                    'File upload error. Exceeded the maximum number of attempts.\n' +
                    f'HTTP STATUS CODE: {response.status_code}'
                )

    def resolve_path(self, remote_path: str):
        """
        :param remote_path: path in format /folder/file.ext
        :return: True if path exists
        """
        endpoint = 'https://cloud-api.yandex.net/v1/disk/resources'
        headers = {'Authorization': f'OAuth {self.token}'}
        response = requests.get(endpoint, headers=headers, params={'path': remote_path.replace('//', '/')})
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        self._is_api_error(response.json())

    def md5(self, remote_path: str):
        """
        :param remote_path: path to the file
        :return: md5 checksum of the file
        """
        endpoint = 'https://cloud-api.yandex.net/v1/disk/resources'
        headers = {'Authorization': f'OAuth {self.token}'}
        response = requests.get(endpoint, headers=headers,
                                params={'fields': 'md5', 'path': remote_path.replace('//', '/')})
        if response.status_code == 404:
            return None
        data = response.json()
        self._is_api_error(data)
        return data.get('md5')

    def files_list(self, folder_path: str, media_type=None, *fields) -> list:
        """
        :param folder_path: path in format /folder/other_folder
        :param media_type: type of files to include in the list, for example 'images' or 'videos'
        :param fields: the list of returned JSON fields to be returned.
        Nested fields are separated by a dot, for example files.name
        :return: list of file info contains specified fields
        """
        folder_path = folder_path if folder_path.startswith('/') else f'/{folder_path}'
        folder_path = folder_path if folder_path.endswith('/') else f'{folder_path}/'
        folder_path = folder_path.replace('//', '/')
        endpoint = 'https://cloud-api.yandex.net/v1/disk/resources/files'
        headers = {'Authorization': f'OAuth {self.token}'}
        params = {'limit': 1000}
        if fields:
            fields_value = ','.join(fields)
            if 'items.path' not in 'fields':
                fields_value += ',items.path'
            if 'items.name' not in 'fields':
                fields_value += ',items.name'
            params['fields'] = fields_value
        if media_type:
            params['media_type'] = media_type
        offset = 0
        result = []
        while True:
            params['offset'] = offset
            response = requests.get(endpoint, headers=headers, params=params)
            data = response.json()
            self._is_api_error(data)
            count = len(data['items'])
            if not count:
                break
            result += [item for item in data['items'] if item['path'] == f'disk:{folder_path}{item["name"]}']
            offset += params['limit']
            if count < params['limit']:
                break
        return result
