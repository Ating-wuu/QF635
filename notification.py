import base64
import hashlib
import json
import os.path
import traceback
from datetime import datetime

import requests

def send_wechat_work_msg(content, url):
    if not url:
        print('check your wechat_webhook_url.')
        return
    try:
        data = {"msgtype": "text", "text": {"content": content + '\n' + datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
        r = requests.post(url, data=json.dumps(data), timeout=10)
    except Exception as e:
        print(traceback.format_exc())


class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)

def send_wechat_work_img(file_path, url):
    if not os.path.exists(file_path):
        return
    if not url:
        return
    try:
        with open(file_path, 'rb') as f:
            image_content = f.read()
        image_base64 = base64.b64encode(image_content).decode('utf-8')
        md5 = hashlib.md5()
        md5.update(image_content)
        image_md5 = md5.hexdigest()
        data = {'msgtype': 'image', 'image': {'base64': image_base64, 'md5': image_md5}}
        r = requests.post(url, data=json.dumps(data, cls=MyEncoder, indent=4), timeout=10)
    except Exception as e:
        print(traceback.format_exc())
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def send_msg_for_order(order_param, order_res, url):
    if not url:
        return
    msg = ''
    try:
        for _ in range(len(order_param)):
            if 'msg' in order_res[_].keys():
                msg += f'token:{order_param[_]["symbol"]}\n'
                msg += f'side:{"long" if order_param[_]["side"] == "BUY" else "short"}\n'
                msg += f'price:{order_param[_]["price"]}\n'
                msg += f'quantity:{order_param[_]["quantity"]}\n'
                msg += f'order result:{order_res[_]["msg"]}'
                msg += '\n' * 2
    except BaseException as e:
        print('send_msg_for_order ERROR', e)
        print(traceback.format_exc())

    if msg:
        send_wechat_work_msg(msg, url)
