import os
import sys
import socket
import argparse
import json
import time
from tqdm import tqdm


DEFAULT_SERVER_ADDRESS = '127.0.0.1'  # デフォルトのサーバーIPアドレス
DEFAULT_SERVER_PORT = 12348            # デフォルトのサーバーポート
STREAM_RATE = 1400             # 送信するチャンクサイズ
HEADER_SIZE = 8               # ヘッダーのサイズ：8バイト(JSON:2,mediatype:1,payload:5バイト)
MAX_JSON_SIZE = 216
MAX_MEDIA_TYPE_SIZE = 4
MAX_PAYLOAD_SIZE = (1<<40) #1TB

'''
クライアントの機能：
ユーザーが選択した動画ファイルをサーバーにアップロード。
処理リクエストをJSON形式でサーバーに送信。
サーバーからのレスポンスを受信し、処理結果のファイルをダウンロード。
'''


def is_mp4(filepath):
    try:
        with open(filepath, 'rb') as f:
            header = f.read(12)
            return header[4:8] == b'ftyp'
    except Exception as e:
        print(f"誤ったファイル形式です: {e}")
        return False

def recv_all(sock, length):
    """指定されたバイト数を確実に受信する関数"""
    data = b""
    while len(data) < length:
        more = sock.recv(length - len(data))
        if not more:
            raise EOFError(f"Expected {length} bytes but received {len(data)} bytes before the connection closed.")
        data += more
    return data

def send_request(sock, json_args, media_type, payload):
    """
    リクエストを送信する関数
    - json_args: JSON形式の引数
    - media_type: メディアタイプ（例: 'mp4'）
    - payload: バイナリデータ
    MMPプロトコルに従う
    """
    json_bytes = json.dumps(json_args).encode('utf-8')
    json_size = len(json_bytes)
    if json_size > MAX_JSON_SIZE:
        raise ValueError('JSON size exceeds max allowed.')

    media_type_bytes = media_type.encode('utf-8')
    media_type_size = len(media_type_bytes)
    if media_type_size > MAX_MEDIA_TYPE_SIZE:
        raise ValueError('Media type size exceeds maximum allowed.')
    payload_size = len(payload)
    if payload_size > MAX_PAYLOAD_SIZE:
        raise ValueError('Payload size exceeds maximum allowed.')

    # ヘッダーの作成
    header = json_size.to_bytes(2, 'big') + bytes ([media_type_size]) + payload_size.to_bytes(5, 'big')
    header += b'\0' * (HEADER_SIZE - 8)  # 余りのバイトをパディング

    # ボディの作成
    body = json_bytes + media_type_bytes
    '''
    ボディの内容
    jsonデータ: リクエストの引数をJSON形式でエンコード
    media_type: ファイルの拡張子をUTF-8でエンコード
    ペイロード: ファイルのバイナリデータ
    '''

    # ヘッダーとボディの送信
    sock.sendall(header + body)



def receive_response(sock):
    '''
    レスポンスを受信する関数
    ヘッダを解析してJSONデータ、メディアタイプ、ペイロードの受信
    - sock: ソケットオブジェクト
    '''
    #ヘッダーの受信
    header = recv_all(sock, HEADER_SIZE)
    json_size = int.from_bytes(header[:2], 'big')
    media_type_size = header[2]
    payload_size = int.from_bytes(header[3:8], 'big')

    #ボディの受信
    json_data = recv_all(sock, json_size).decode('utf-8') if json_size > 0 else ''
    media_type = recv_all(sock, media_type_size).decode('utf-8') if media_type_size > 0 else ''
    payload = recv_all(sock, payload_size) if payload_size > 0 else b''

    return json_data, media_type, payload


def upload_file(server_address, server_port, filepath, operation, args, status_interval = 60):
    if not os.path.isfile(filepath):
        print(f"Error: ファイル{filepath}が見つかりません")
        sys.exit(1)

    if not filepath.lower().endswith('.mp4'):
        print("mp4拡張子しか扱っていません")
        sys.exit(1)

    if not is_mp4(filepath):
        print("Error: The file is not a valid MP4 file.")
        sys.exit(1)

    filesize = os.path.getsize(filepath)
    if filesize > (1 << 32):  # 4GB制限
        print("Error: File must be below 4GB.")
        sys.exit(1)

    #リクエストJSONの作成
    json_args = {
        'operation': operation,
        **args
    }

    # 拡張子からメディアタイプの取得
    media_type = os.path.splitext(filepath)[1][1:].lower()

    #ファイルの読み込み
    with open(filepath, 'rb') as f:
        payload = f.read()

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((server_address, server_port))
            print(f"サーバーに接続しました {server_address}:{server_port}")

            #リクエストを送信
            send_request(sock, json_args, media_type, payload)
            print(f"送信予定のファイルサイズ: {filesize}バイト")

            # ファイルデータの送信（進捗表示付き）
            print("ファイルを送信中...")
            with open(filepath, 'rb') as f:
                total_sent = 0
                with tqdm(total=filesize, unit='B', unit_scale=True, desc='Sending') as pbar:
                    while True:
                        data = f.read(STREAM_RATE)
                        if not data:
                            break
                        sock.sendall(data)
                        total_sent += len(data)
                        pbar.update(len(data))
            print("ファイルの送信に成功しました")

            # レスポンスを受信
            print("サーバーからのレスポンスを受信中...")
            json_response, response_media_type, response_payload = receive_response(sock)
            response = json.loads(json_response) if json_response else {}
            if response.get('status') == 'success':
                output_filename = f"processed_{os.path.basename(filepath)}"
                with open(output_filename, 'wb') as out_f:
                    out_f.write(response_payload)
                print(f"処理完了。ダウンロードされたファイル: {output_filename}")
            else:
                error_message = response.get('message', 'Unknown error.')
                print(f"サーバーからのエラー: {error_message}")

    except socket.error as e:
        print(f"socket error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"エラーが発生しました: {e}")
    finally:
        print("ソケットを閉じました")


def main():
    parser = argparse.ArgumentParser(description='Upload an MP4 file to the server.')
    parser.add_argument('filepath', help='Path to the MP4 file to upload')
    parser.add_argument('--server', default=DEFAULT_SERVER_ADDRESS, help='Server IP address (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=DEFAULT_SERVER_PORT, help='Server port number (default: 12345)')
    parser.add_argument('--operation', required=True, choices=[
        'compress',
        'change_resolution',
        'change_aspect_ratio',
        'extract_audio',
        'create_gif',
        'create_webm'
    ], help='動画処理の操作を指定')
    parser.add_argument('--width', type=int, help='解像度変更時の幅')
    parser.add_argument('--height', type=int, help='解像度変更時の高さ')
    parser.add_argument('--aspect_ratio', help='アスペクト比変更時の比率（例: "16:9"）')
    parser.add_argument('--start_time', type=float, help='GIF/WebM作成時の開始時間（秒）')
    parser.add_argument('--duration', type=float, help='GIF/WebM作成時の期間（秒）')

    args = parser.parse_args()
    # 操作に応じた追加引数の設定
    operation_args = {}
    if args.operation == 'change_resolution':
        if args.width is None or args.height is None:
            print("Error: --width と --height は change_resolution 操作に必要です。")
            sys.exit(1)
        operation_args['width'] = args.width
        operation_args['height'] = args.height
    elif args.operation == 'change_aspect_ratio':
        if args.aspect_ratio is None:
            print("Error: --aspect_ratio は change_aspect_ratio 操作に必要です。")
            sys.exit(1)
        operation_args['aspect_ratio'] = args.aspect_ratio
    elif args.operation in ['create_gif', 'create_webm']:
        if args.start_time is None or args.duration is None:
            print(f"Error: --start_time と --duration は {args.operation} 操作に必要です。")
            sys.exit(1)
        operation_args['start_time'] = args.start_time
        operation_args['duration'] = args.duration

    upload_file(
        server_address=args.server,
        server_port=args.port,
        filepath=args.filepath,
        operation=args.operation,
        args=operation_args
    )


if __name__ == "__main__":
    main()
