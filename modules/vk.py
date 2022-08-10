import io
from time import sleep
import requests
from datetime import datetime
from modules.oauth import VKToken


# RPS - requests per second
VK_API_MAX_RPS = 3
# IPR - Items Per Request
VK_PHOTOS_MAX_IPR = 1000
VK_MAX_REQUEST_COUNT_IN_CODE = 25
VK_PHOTO_SIZE_TYPES = 'smxopqryzw'


class VKRPSLimitError(Exception):
    pass


class VKInternalError(Exception):
    pass


class VKProfileAccessError(Exception):
    pass


class VKProfileDeletedError(Exception):
    pass


VK_ERROR_LIST = {
    6: VKRPSLimitError,
    10: VKInternalError,
    18: VKProfileDeletedError,
    30: VKProfileAccessError
}


class VKAPI:
    version = '5.131'

    def __init__(self, oauth_token: VKToken):
        self.oauth = oauth_token
        self._request_log = []

    def _is_error(self, data: dict):
        _ = self
        if 'error' not in data:
            return False
        error_code = data["error"]["error_code"]
        exception = VK_ERROR_LIST.get(error_code) or Exception
        raise exception(f'VK API ERROR {data["error"]["error_code"]}: {data["error"]["error_msg"]}')

    def _check_rps(self):
        now = datetime.now().timestamp()
        self._request_log = [timestamp for timestamp in self._request_log if now - timestamp <= 1]
        if len(self._request_log) == VK_API_MAX_RPS:
            delta = now - self._request_log[0]
            if delta <= 1:
                sleep(1.001 - delta)
        self._request_log.append(datetime.now().timestamp())

    def _get(self, method, params, headers=None):
        self._check_rps()
        response = requests.get(
            url=f'https://api.vk.com/method/{method}',
            params={
                'access_token': self.oauth.access_token,
                'v': self.version,
                **params
            },
            headers=headers
        )
        data = response.json()
        if not self._is_error(data):
            return data['response']

    def users_get(self, user_ids: list, fields: list, name_case='nom') -> dict:
        """
        :param user_ids: VK user ids and(or) short names(screen_name)
        :param fields: list of fields to get, more info: https://dev.vk.com/reference/objects/user
        :param name_case: the case for declension of the user's first and last name,
        possible values: nom, gen, dat, acc, ins, abl
        :return: information about the users
        """
        if name_case not in ['nom', 'gen', 'dat', 'acc', 'ins', 'abl']:
            name_case = 'nom'
        params = {'name_case': name_case}
        if user_ids and len(user_ids):
            params['user_ids'] = ','.join([str(user_id) for user_id in user_ids] if len(user_ids) else None)
        if fields and len(fields):
            params['fields'] = ','.join(fields)
        return self._get('users.get', params)

    def photos_get(self, owner_id: int, album_id='profile', **params):
        """
        :param owner_id: vk user id (photo owner)
        :param album_id: photo album id
        :param params: additional params: https://dev.vk.com/method/photos.get
        :return: information about the user's(owner_id) photos
        """
        return self._get('photos.get', params={
            'owner_id': owner_id,
            'album_id': album_id,
            **params
        })

    def execute(self, code):
        """
        :param code: algorithm code in VKScript format, more info: https://dev.vk.com/method/execute
        :return: code result, server response in json format
        """
        return self._get('execute', params={'code': code})

    def photos_get_albums(self, owner_id, album_ids: list, offset=0, count=None, **params):
        """
        :param owner_id: vk user id (photo album owner)
        :param album_ids: photo album ids, if the ids array is empty, all photo albums will be returned
        :param offset: the offset required to select a specific subset of albums
        :param count: count of albums
        :param params: additional params: https://dev.vk.com/method/photos.getAlbums
        :return: information about the user's(owner_id) photo albums
        """
        params = {'owner_id': owner_id, 'offset': offset, **params}
        if album_ids and len(album_ids):
            params['album_ids'] = ','.join(map(str, album_ids))
        if count:
            params['count'] = count
        return self._get('photos.getAlbums', params=params)


class VK:
    def __init__(self, api: VKAPI):
        self.api = api

    def users(self, ids, fields: list):
        data = self.api.users_get(ids, fields)
        return [VKUser(self, user_id=item['id'], **item) for item in data]

    def user(self, user_id=None, fields: list = []):
        return self.users(ids=[user_id] if user_id else None, fields=fields).pop()


class VKUser:
    def __init__(self, root: VK, user_id, first_name, last_name, is_closed=None, can_access_closed=None,
                 deactivated=False, **kwargs):
        self.root = root
        self._id = user_id
        self._first_name = first_name
        self._last_name = last_name
        self._deactivated = bool(deactivated)
        self._is_closed = True if is_closed is None else bool(is_closed)
        self._can_access_closed = True if can_access_closed is None else bool(can_access_closed)

    @property
    def id(self):
        return self._id

    @property
    def first_name(self):
        return self._first_name

    @property
    def last_name(self):
        return self._last_name

    @property
    def deactivated(self) -> bool:
        return self._deactivated

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    @property
    def can_access_closed(self) -> bool:
        return self._can_access_closed

    def photo_albums(self, offset=0, count=None) -> list:
        """
        :param offset: the offset required to select a specific subset of albums
        :param count: count of albums
        :return: list of VKPhotoAlbum instances
        """
        if not self._can_access_closed:
            raise VKProfileAccessError("This profile is private")
        if self._deactivated:
            raise VKProfileDeletedError("This profile is deactivated")
        data = self.root.api.photos_get_albums(self.id, album_ids=None, offset=offset, count=count)
        if 'items' in data and len(data['items']):
            return [VKPhotoAlbum(self.root, self, album_id=item['id'], **item) for item in data['items']]

    def get_photos(self, offset=0, count=None, sort_desc=True) -> list:
        """
        :param offset: the offset required to select a specific subset of photos
        :param count: number of photos (all by default)
        :param sort_desc: sort order (recent photos if True)
        :return: returns profile photos, list of VKPhoto instances
        """
        data = self.root.api.photos_get(self._id, album_id='profile', offset=offset, count=count, rev=sort_desc,
                                        photo_sizes=True, extended=True)
        if 'items' in data and len(data['items']):
            return [VKPhoto(self.root, self, **{'photo_id': photo['id'], **photo}) for photo in data['items']]

    def get_photos_from_albums(self, albums: list, photo_offset=0, photo_count=None, extended=True) -> list:
        """
        :param albums: each album in albums[] array should be the VKPhotoAlbum instance
        :param photo_offset: the offset from which the photos in photo album will be received
        :param photo_count: the number of photos to be received from each photo album
        :param extended: get extended information about photos
        :return: list of VKPhoto instances
        """
        if any([True for album in albums if not isinstance(album, VKPhotoAlbum)]):
            raise TypeError('Each album in albums[] array should be the VKPhotoAlbum instance.')
        result = []
        code = ''
        limit = None
        for album in albums:
            limit = VK_MAX_REQUEST_COUNT_IN_CODE
            count = photo_count - photo_offset if photo_count else album.photo_count - photo_offset
            for i in range(photo_offset, count + photo_offset, VK_PHOTOS_MAX_IPR):
                estimate_count = count - i
                params = {
                    'owner_id': self._id,
                    'album_id': album.id,
                    'extended': 1 if extended else 0,
                    'photo_sizes': 1,
                    'offset': i,
                    'count': estimate_count if estimate_count < VK_PHOTOS_MAX_IPR else VK_PHOTOS_MAX_IPR
                }
                code += (
                    f'var photos = API.photos.get({str(params)});\n'
                    'result.items = result.items + photos.items;\n'
                    'result.count = result.count + photos.count;\n'
                )
                limit -= 1
                if not limit:
                    data = self.root.api.execute(f'var result = {{"count": 0, "items": []}};\n{code}\nreturn result;')
                    result += [VKPhoto(self.root, self._id, photo_id=photo['id'], **photo) for photo in data['items']]
                    code = ''
                    limit = VK_MAX_REQUEST_COUNT_IN_CODE
        if limit and limit != VK_MAX_REQUEST_COUNT_IN_CODE:
            data = self.root.api.execute(code=f'var result = {{"count": 0, "items": []}};\n{code}\nreturn result;')
            result += [VKPhoto(self.root, self._id, photo_id=photo['id'], **photo) for photo in data['items']]
        return result


class VKPhotoSize:
    def __init__(self, url, width, height, size_type=None, **kwargs):
        self.url = url
        self.width = int(width)
        self.height = int(height)
        self.type = size_type

    def __gt__(self, other):
        if not isinstance(other, VKPhotoSize):
            raise Exception('Can not compare different types.')
        if self.type and other.type:
            return VK_PHOTO_SIZE_TYPES.index(self.type) > VK_PHOTO_SIZE_TYPES.index(other.type)
        return self.width * self.height > other.width * other.height

    def __str__(self):
        return self.type

    @property
    def file_extension(self):
        start_index = self.url.rfind('/') + 1
        name = self.url[start_index:self.url.find('?', start_index)]
        return name[name.rfind('.') + 1:]

    @property
    def mime_type(self):
        response = requests.head(self.url)
        if response.status_code == 200:
            return response.headers.get('Content-Type')
        else:
            raise Exception('Link broken')

    def read_bytes(self, buffer: io.BufferedWriter = None) -> io.BufferedWriter:
        """
        :param buffer: the buffer to which the image data will be written.
        If not passed, it will be created and the carriage will be set to the beginning of the buffer.
        :return: buffer (if passed) or io.BytesIO instance
        """
        reset_carriage = buffer is None
        buffer = buffer or io.BytesIO()
        response = requests.get(self.url, stream=True)
        if response.status_code == 200:
            for chunk in response.iter_content(1024 ** 2):
                buffer.write(chunk)
            if reset_carriage:
                buffer.seek(0)
            return buffer
        else:
            raise Exception('Broken link')


class VKPhotoAlbum:
    def __init__(self, root: VK, owner: VKUser, album_id, title, description, created, updated, size: int, **kwargs):
        self.root = root
        self.owner = owner
        self._id = album_id
        self._title = title
        self._description = description
        self._created = datetime.fromtimestamp(created) if created else None
        self._updated = datetime.fromtimestamp(updated) if updated else None
        self._size = size

    def __repr__(self):
        return self.title

    @property
    def id(self):
        return self._id

    @property
    def title(self):
        return self._title

    @property
    def description(self):
        return self._description

    @property
    def creation_timestamp(self):
        return self._created

    @property
    def last_update_timestamp(self):
        return self._updated

    @property
    def photo_count(self):
        return self._size

    def get_photos(self, offset=0, count=None, sort_desc=True):
        """
        :param offset: the offset required to select a specific subset of photos
        :param count: if not set, all will return
        :param sort_desc: if true, the last photos are returned
        :return: photos from the album according to the specified offset and count
        """
        count = count or self._size - offset
        result = []
        for offset in range(offset, count + offset, VK_PHOTOS_MAX_IPR):
            estimate_count = count if count < VK_PHOTOS_MAX_IPR else VK_PHOTOS_MAX_IPR
            data = self.root.api.photos_get(
                self.owner.id, self._id, offset=offset, count=estimate_count,
                rev=sort_desc, photo_sizes=True, extended=True
            )
            if 'items' in data and len(data['items']):
                result += [
                    VKPhoto(self.root, self.owner, **{'photo_id': photo['id'], **photo}) for photo in data['items']
                ]
            else:
                break
        return result


class VKPhoto:
    def __init__(self, root: VK, owner: VKUser, photo_id, album_id, text, date, sizes, likes=None, **kwargs):
        self.root = root
        self.owner = owner
        self.sizes = [VKPhotoSize(size_type=size['type'], **size) for size in sizes]
        self.id = photo_id
        self.album_id = album_id
        self.text = text
        self.date = datetime.fromtimestamp(date)
        self.likes_count = likes['count'] if likes else None

    @property
    def max_size(self):
        return max(self.sizes)
