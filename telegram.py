import time
import urllib.parse 
import urllib.request

class Telegram:
    def __init__(self, secret, chat_id):
        self.messages = []
        self.secret = secret
        self.chat_id = chat_id

    def new_message(self, message):
        self.messages.append(message)

    def start(self):
        while True:
            if self.messages:
                msg = self.messages
                self.messages = self.messages[1:]
                while msg:
                    try:
                        self._telegram_inform(msg)
                        msg = False
                    except:
                        time.sleep(1)
            time.sleep(1)

    def _telegram_inform(self, message: str):
        params = {
            "parse_mode": "markdown",
            "text": message,
            "chat_id": self.chat_id
        }
        query_string = urllib.parse.urlencode(params)
        data = query_string.encode("ascii")
        url = "https://api.telegram.org/bot%s/sendMessage" % self.secret

        with urllib.request.urlopen(url, data) as response: response.read()
