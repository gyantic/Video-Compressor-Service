import socket
import os
import ffmpeg
import time
import json
import logging
import uuid  #出力ファイルにユニバーサリー一意の識別子を割り振る
import threading
from threading import Lock

#ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

SERVER_ADDRESS = '0.0.0.0'
SERVER_PORT = 12345
STREAM_RATE = 1400
HEADER_SIZE = 8 #64ビット
MAX_JSON_SIZE = 216
MAX_MEDIA_TYPE_SIZE = 4
MAX_PAYLOAD_SIZE = (1 << 40) #1TB

RESPONSE_SIZE = 16

UPLOAD_DIR = 'uploads'
os.makedirs(UPLOAD_DIR, exist_ok=True)

# IPアドレスごとの処理制限
active_ips = {}
active_ips_lock = Lock()

#ジョブステータスの管理
jobs_status = {}
jobs_lock = Lock()
#------------------------------------------------------
'''
サーバー側でクライアントが送信してきた動画の処理を行う
1.動画の圧縮
2.動画の解像度を変更
3.アスペクト比を変更
4.動画→音声
5.GIF,WEBMフォーマットに変換
'''
#--------------------------------------------------------
def recv_all(connection, length):
    """指定されたバイト数を確実に受信する関数"""
    data = b''
    while len(data) < length:
        more = connection.recv(length - len(data))
        if not more:
            raise EOFError("Expected {length} bytes but received {len(data)} bytes before the connection closed.")
        data += more
    return data


def send_response(connection, status, message, media_type, payload):
    """
    最終レスポンスを送信
    """
    try:
        # JSON レスポンス作成
        response_json = json.dumps({"status": status, "message": message}).encode("utf-8")
        json_size = len(response_json)
        media_type_encoded = media_type.encode("utf-8")
        media_type_size = len(media_type_encoded)
        payload_size = len(payload)

        # サイズの検証
        if json_size > MAX_JSON_SIZE or media_type_size > MAX_MEDIA_TYPE_SIZE or payload_size > MAX_PAYLOAD_SIZE:
            raise ValueError("レスポンスのサイズが制限を超えています。")

        # ヘッダー作成
        header = (
            json_size.to_bytes(2, "big")
            + bytes([media_type_size])
            + payload_size.to_bytes(5, "big")
        )

        # ヘッダー + ボディ送信
        connection.sendall(b"FINAL:\n")
        connection.sendall(header)
        connection.sendall(response_json + media_type_encoded + payload)
        logging.info(f"最終レスポンスを送信しました: status={status}, payload_size={payload_size}")
    except Exception as e:
        logging.error(f"最終レスポンス送信エラー: {e}")


def send_progress(connection, stop_event):
    """
    定期的に進行中メッセージを送信 (改行付きで行として送る)
    """
    try:
        while not stop_event.is_set():
            time.sleep(10)  # 10秒ごとに進行中メッセージを送信
            progress_message = "PROGRESS: 作業中です...\n"
            connection.sendall(progress_message.encode('utf-8'))
    except Exception as e:
        logging.error(f"進行中メッセージ送信中にエラーが発生しました: {e}")





def handle_client(connection, client_address):
    ip = client_address[0]

    with active_ips_lock:
        if active_ips.get(ip, 0) >= 1:
            logging.info(f"IP {ip} からの複数同時処理リクエストを拒否しました。")
            send_response(connection, 'error', 'Only one processing per IP is allowed.', '', b'')
            connection.close()
            return
        else:
            active_ips[ip] = active_ips.get(ip, 0) + 1


    stop_event = threading.Event()
    progress_thread = threading.Thread(target=send_progress, args=(connection, stop_event))
    try:
        # ヘッダーの受信
        header = recv_all(connection, HEADER_SIZE)
        json_size = int.from_bytes(header[:2], 'big')
        media_type_size = header[2]
        payload_size = int.from_bytes(header[3:8], 'big')

        # ボディの受信
        json_data = recv_all(connection, json_size).decode('utf-8')
        media_type = recv_all(connection, media_type_size).decode('utf-8')
        payload = recv_all(connection, payload_size)

        logging.info(f"リクエスト受信: operation={json.loads(json_data).get('operation')}, media_type={media_type}, payload_size={payload_size} bytes")
        progress_thread.start()

        # 動画処理の実行
        result = process_request(json_data, media_type, payload)

        # 処理完了時に進捗スレッドを停止
        stop_event.set()
        progress_thread.join()

        # 成功レスポンスの送信
        send_response(connection, result['status'], result['message'], result['media_type'], result['payload'])
        logging.info(f"処理完了: {result['media_type']}, payload_size={len(result['payload'])} bytes")

    except Exception as e:
        logging.error(f"エラー発生: {e}")
        send_response(connection, 'error', str(e), '', b'')

    finally:
        connection.close()
        with active_ips_lock:
            active_ips[ip] -= 1
            if active_ips[ip] == 0:
                del active_ips[ip]
        logging.info(f"クライアント {client_address} との接続を閉じました。")


def process_request(json_args, media_type, payload):
    #一時ファイルの作成
    input_filename = f"input_{uuid.uuid4()}.{media_type}"
    input_path = os.path.join(UPLOAD_DIR, input_filename)
    with open(input_path, 'wb') as f:
        f.write(payload)

    #処理結果のファイルパス
    output_filename = f"output_{uuid.uuid4()}.mp4"  # 出力形式は要件に応じて変更
    output_path = os.path.join(UPLOAD_DIR, output_filename)
    try:
        '''動画処理の実装'''
        args = json.loads(json_args)
        operation = args.get('operation')
        if operation == 'compress':
            (
                ffmpeg
                .input(input_path)
                .output(output_path, video_bitrate='500k')
                .run(overwrite_output = True)
            )
        elif operation == 'change_resolution':
            width = args.get('width')
            height = args.get('height')
            if not width or not height:
                raise ValueError('Width and height must be specified for resolution change.')
            (
                ffmpeg
                .input(input_path)
                .filter('scale', width, height)
                .output(output_path)
                .run(overwrite_output = True)
            )
        elif operation == 'change_aspect_ratio':
            aspect_ratio = args.get('aspect_ratio')
            if not aspect_ratio:
                return ValueError('Aspect ratio must be specified.')
            (
                ffmpeg
                .input(input_path)
                .filter('setsar', '1')
                .filter('setdar', aspect_ratio) #set dislay aspect ratio
                .output(output_path)
                .run(overwrite_output = True)
            )
        elif operation == 'extract_audio':
            output_filename = f"audio_{uuid.uuid4()}.mp3"
            output_path = os.path.join(UPLOAD_DIR, output_filename)
            (
                ffmpeg
                .input(input_path)
                .output(output_path, format='mp3', acodec='libmp3lame', ab='192k')
                .run(overwrite_output = True)
            )
        elif operation == 'create_gif':
            start_time = args.get('start_time')
            duration = args.get('duration')
            if start_time is None or duration is None:
                raise ValueError('Start time and duration must be specified.')
            output_filename = f"gif_{uuid.uuid4()}.gif"
            output_path = os.path.join(UPLOAD_DIR, output_filename)
            (
                ffmpeg
                .input(input_path, ss=start_time, t=duration)
                .output(output_path, vf='fps=10,scale=320:-1:flags=lanczos')
                .run(overwrite_output=True)
            )
        elif operation == 'create_webm':
            start_time = args.get('start_time')
            duration = args.get('duration')
            if start_time is None or duration is None:
                raise ValueError('Start time and duration must be specified for WEBM creation.')
            output_filename = f"webm_{uuid.uuid4()}.webm"
            output_path = os.path.join(UPLOAD_DIR, output_filename)
            (
                ffmpeg
                .input(input_path, ss=start_time, t=duration)
                .output(output_path, format='webm')
                .run(overwrite_output=True)
            )
        else:
            raise ValueError('Unsupported operation.')



        #出力を読み込んで返す
        with open(output_path, 'rb') as f:
            output_payload = f.read()


        os.remove(input_path)
        return {'status': 'success', 'message': 'Processing completed.', 'media_type': os.path.splitext(output_filename)[1][1:], 'payload': output_payload}
    except Exception as e:
        os.remove(input_path)
        raise e


def start_server():
    """サーバーを起動する関数"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((SERVER_ADDRESS,SERVER_PORT))
        sock.listen(5)
        logging.info(f"サーバーが {SERVER_ADDRESS}:{SERVER_PORT} で待機中です。")

        #クライアントの接続待ち状態
        while True:
            try:
                connection, client_address = sock.accept()
                client_thread = threading.Thread(target = handle_client, args=(connection, client_address))
                client_thread.daemon = True # メインプログラムの終了時にスレッドも終了
                client_thread.start()
            except KeyboardInterrupt:
                logging.info("サーバーを停止します")
                break
            except Exception as e:
                logging.error(f"接続待機中にエラーが発生しました: {e}")


if __name__ == "__main__":
    start_server()
