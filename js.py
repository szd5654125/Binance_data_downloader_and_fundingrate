import requests
import xml.etree.ElementTree as ET
import os
import hashlib
import zipfile


def calculate_file_hash(file_path, hash_algo='sha256'):
    """calc hash"""
    hash_obj = hashlib.new(hash_algo)
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def unzip_file(zip_file_path, extract_to_folder):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)


def download_verification_unzip_fundingRate(um_or_cm,signal):
    # set upper folder name
    parent_folder_name = f'{um_or_cm}_data'
    # make sure upper folder exist
    parent_folder_path = os.path.join(os.getcwd(), parent_folder_name)
    if not os.path.exists(parent_folder_path):
        os.makedirs(parent_folder_path)

    # in upper folder create new folder
    folder_name = f'futures_{um_or_cm}_monthly_fundingRate_{signal}_zip'
    # change folder_path to include upper folder paths
    folder_path = os.path.join(parent_folder_path, folder_name)

    # if not exist create
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # URL
    s3_rest_url = f'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/{um_or_cm}/monthly/fundingRate/{signal}/'

    response = requests.get(s3_rest_url)

    # check
    if response.status_code == 200:
        # parsing XML
        root = ET.fromstring(response.content)

        # print each Key
        for contents in root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}Contents'):
            key = contents.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text

            # build download URL
            download_url = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision' + '/' + key

            # use file name and new folder create local path
            local_file_path = os.path.join(folder_path, key.split('/')[-1])

            # send request and save data
            response = requests.get(download_url)
            if response.status_code == 200:
                with open(local_file_path, 'wb') as f:
                    f.write(response.content)
                print(f'File {local_file_path} downloaded successfully.')
            else:
                print(f'Failed to download file {local_file_path}, status code: {response.status_code}')
        # saved data path
        extract_folder_name = f'futures_{um_or_cm}_monthly_fundingRate_{signal}'
        extract_folder_path = os.path.join(parent_folder_path, extract_folder_name)

        # make sure exist
        if not os.path.exists(extract_folder_path):
            os.makedirs(extract_folder_path)

        # through the files in the folder
        for filename in os.listdir(folder_path):
            if filename.endswith('.zip'):
                zip_file_path = os.path.join(folder_path, filename)
                checksum_file_path = zip_file_path + '.CHECKSUM'

                # check CHECKSUM files
                if os.path.exists(checksum_file_path):
                    # calc hash
                    file_hash = calculate_file_hash(zip_file_path)

                    # read hash from CHECKSUM
                    with open(checksum_file_path, 'r') as f:
                        checksum_line = f.readline()
                        checksum_hash = checksum_line.split()[0]  # 假设哈希值在行的开头

                    # compare
                    if file_hash == checksum_hash:
                        print(f'File {filename} checksum verification succeeded.')
                        # un_zip files
                        unzip_file(zip_file_path, extract_folder_path)
                        print(f'File {filename} extracted to {extract_folder_path}.')
                    else:
                        print(f'File {filename} checksum verification failed.')
                else:
                    print(f'CHECKSUM file for {filename} not found.')

    else:
        print(f'Failed to get data, status code: {response.status_code}')


def get_coin_list(um_or_cm):
    # build request URL
    s3_rest_url = f'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/{um_or_cm}/monthly/fundingRate/'

    response = requests.get(s3_rest_url)

    coin_list = []
    if response.status_code == 200:
        # parsing XML
        root = ET.fromstring(response.content)

        # read all CommonPrefixes get coin list
        for common_prefix in root.findall('{http://s3.amazonaws.com/doc/2006-03-01/}CommonPrefixes'):
            prefix = common_prefix.find('{http://s3.amazonaws.com/doc/2006-03-01/}Prefix').text
            # get coin name
            coin = prefix.split('/')[-2]  # 假设币种名称前后都有斜杠（/）
            # if connection breaks in the middle
            # if coin > "KEYUSDT" and coin[0].isalpha():
            coin_list.append(coin)
    return coin_list

def process_coins(um_or_cm):
    coin_list = get_coin_list(um_or_cm)
    for coin in coin_list:
        download_verification_unzip_fundingRate(um_or_cm, coin)
