import webbrowser
import requests
import socketserver
import http.server
from datetime import datetime


GOOGLE_CLIENT_ID = '951735044428-10o3aa58i5a1bln54k52uhq5bceidpi6.apps.googleusercontent.com'
GOOGLE_CLIENT_SECRET = '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'
VK_CLIENT_ID = '8232909'
VK_TOKEN_VALIDATION_API_VERSION = '5.131'


def parse_url_get_params(url):
    result = {}
    if '?' not in url:
        return result
    params = url.split('?')[1]
    for param in params.split('&'):
        key, value = param.split('=')
        result[key] = value
    return result


class OAuthToken:
    def __init__(self, access_token, refresh_token=None, expires_at=None, redirect_uri=None):
        self._access_token = access_token
        self._refresh_token = refresh_token
        self.expires_at = expires_at
        self.redirect_uri = redirect_uri

    @staticmethod
    def from_dict(params: dict, class_name):
        if params is None:
            return None
        access_token = params.get('access_token')
        if not access_token:
            return None
        kwargs = {'access_token': access_token}
        for key in ['refresh_token', 'expires_at', 'redirect_uri']:
            if key in params:
                kwargs[key] = params[key]
        return class_name(**kwargs)

    @property
    def access_token(self) -> str:
        """
        :return: access token value
        """
        if self.is_expired():
            if self.refresh():
                return self._access_token
        else:
            return self._access_token

    def to_dict(self):
        return {
            'access_token': self.access_token,
            'refresh_token': self._refresh_token,
            'expires_at': self.expires_at,
            'redirect_uri': self.redirect_uri
        }

    def is_expired(self, elapsed: int = 10) -> bool:
        """
        :param elapsed: the minimum token lifetime in seconds, by default 10
        :return: True if the token has expired
        """
        if not self.expires_at:
            return False
        return self.expires_at - datetime.now().timestamp() < elapsed

    def is_valid(self) -> bool:
        """Method should be override"""
        pass

    def refresh(self):
        """Method should be override"""
        pass


class GoogleToken(OAuthToken):
    def is_valid(self) -> bool:
        """
        :return: True if GoogleToken is valid now
        """
        headers = {'Authorization': f'Bearer {self.access_token}'}
        response = requests.get('https://www.googleapis.com/drive/v3/files', headers=headers)
        data = response.json()
        if 'error' in data:
            return True if self.refresh() else False
        if response.status_code == 200:
            return True

    def refresh(self):
        """
        :return: access token value if token refreshed successfully
        """
        if not self._refresh_token:
            return None
        response = requests.post('https://oauth2.googleapis.com/token', params={
            'refresh_token': self._refresh_token,
            'client_id': GOOGLE_CLIENT_ID,
            'client_secret': GOOGLE_CLIENT_SECRET,
            'redirect_uri': self.redirect_uri,
            'grant_type': 'refresh_token'
        })
        data = response.json()
        if 'access_token' not in data:
            return None
        self._access_token = data['access_token']
        self.expires_at = datetime.now().timestamp() + int(data['expires_in'])
        return self._access_token

    @staticmethod
    def from_dict(params: dict):
        return OAuthToken.from_dict(params, GoogleToken)


class VKToken(OAuthToken):
    def is_valid(self):
        """
        :return: True if VKToken is valid now
        """
        response = requests.get(
            url=f'https://api.vk.com/method/users.get',
            params={
                'access_token': self.access_token,
                'v': VK_TOKEN_VALIDATION_API_VERSION,
                'user_ids': 1
            }
        )
        data = response.json()
        if 'error' in data:
            if data['error']['error_code'] == 5:
                return False
            raise Exception(f'VK API ERROR {data["error"]["error_code"]}: {data["error"]["error_msg"]}')
        return True

    @staticmethod
    def from_dict(params: dict):
        return OAuthToken.from_dict(params, VKToken)


class TCPServer(socketserver.TCPServer):
    def __init__(self, *args):
        super().__init__(*args)
        self.storage = {}


class VKHandler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        print(self.headers)
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        params = parse_url_get_params(self.path)
        if 'access_token' in params:
            self.server.storage['access_token'] = params['access_token']
            self.wfile.write(
                '<html><head><title>Получение токена</title></head><body>'
                '<h2>Токен успешно получен. Страницу можно закрыть.</h2></body></html'.encode("utf-8")
            )
        else:
            self.wfile.write(
                '''<html><head><title>Получение токена</title>
        <script>
            document.addEventListener('DOMContentLoaded', function(){
                window.location.href = '/?' + window.location.href.split('#')[1]  
            })
        </script></head><body><h2>Попытка получить токен</h2></body></html>'''.encode("utf-8")
            )

    def log_message(self, *args):
        return


class GoogleHandler(http.server.BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        code = parse_url_get_params(self.path).get('code')
        if not code:
            raise Exception('Error while getting token')
        self.server.storage['code'] = code
        self.wfile.write(
            '<html><head><title>Получение токена</title></head><body>'
            '<h2>Токен успешно получен. Страницу можно закрыть.</h2></body></html'.encode("utf-8")
        )

    def log_message(self, *args):
        return


def receive_token_vk():
    uri = f'https://oauth.vk.com/authorize?client_id={VK_CLIENT_ID}&scope=65536&response_type=token'
    with TCPServer(('127.0.0.1', 0), VKHandler) as httpd:
        redirect_uri = f'http://localhost:{httpd.socket.getsockname()[1]}'
        webbrowser.open(f'{uri}&redirect_uri={redirect_uri}')
        while True:
            if 'access_token' in httpd.storage:
                return VKToken(httpd.storage['access_token'])
            httpd.handle_request()


def receive_token_google():
    uri = (
        'https://accounts.google.com/o/oauth2/auth?scope=https://www.googleapis.com/auth/drive&response_type=code'
        f'&access_type=offline&client_id={GOOGLE_CLIENT_ID}'
    )
    with TCPServer(('127.0.0.1', 0), GoogleHandler) as httpd:
        redirect_uri = f'http://localhost:{httpd.socket.getsockname()[1]}'
        webbrowser.open(f'{uri}&redirect_uri={redirect_uri}')
        while True:
            if 'code' in httpd.storage:
                response = requests.post('https://oauth2.googleapis.com/token', params={
                    'code': httpd.storage['code'],
                    'client_id': GOOGLE_CLIENT_ID,
                    'client_secret': GOOGLE_CLIENT_SECRET,
                    'redirect_uri': redirect_uri,
                    'grant_type': 'authorization_code'
                })
                data = response.json()
                if 'access_token' not in data:
                    raise Exception('Error while getting token')
                return GoogleToken(
                    access_token=data['access_token'],
                    refresh_token=data['refresh_token'],
                    expires_at=datetime.now().timestamp() + data['expires_in'],
                    redirect_uri=redirect_uri
                )
            httpd.handle_request()
