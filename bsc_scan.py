import json
import requests
import pandas as pd
from datetime import datetime, timedelta, time
import os
from dotenv import load_dotenv
import math
import time
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bsc_scan.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 加载环境变量
load_dotenv()

# BSCScan API配置
BSCSCAN_API_KEY = os.getenv('BSCSCAN_API_KEY')
BSCSCAN_API_URL = "https://api.bscscan.com/api"

# WxPusher Configuration
WXPUSHER_APP_TOKEN = os.getenv('WXPUSHER_APP_TOKEN')
WXPUSHER_UID = os.getenv('WXPUSHER_UID')

# USDT合约地址（BSC上的USDT）
USDT_CONTRACT = "0x55d398326f99059fF775485246999027B3197955"

# DEX Router合约地址
DEX_ROUTER = "0xb300000b72DEAEb607a12d5f54773D1C19c7028d"

def load_wallets():
    """加载钱包地址和别名"""
    try:
        with open('wallets.json', 'r', encoding='utf-8') as f:
            wallets_data = json.load(f)
            return wallets_data.get('wallets', [])
    except FileNotFoundError:
        logging.error("Error: wallets.json not found.")
        return []
    except json.JSONDecodeError:
        logging.error("Error: Could not parse wallets.json. Please check the file format.")
        return []
    except Exception as e:
        logging.error(f"Error loading wallets.json: {e}")
        return []

def get_wallet_transactions(address, start_timestamp, end_timestamp):
    """获取钱包在指定时间范围内的交易"""
    params = {
        'module': 'account',
        'action': 'txlist',
        'address': address,
        'startblock': 0,
        'endblock': 99999999,
        'page': 1,
        'offset': 1000,
        'sort': 'desc',
        'apikey': BSCSCAN_API_KEY
    }
    
    response = requests.get(BSCSCAN_API_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        if data['status'] == '1':
            return data['result']
    return []

def get_usdt_token_transfers(address, start_timestamp, end_timestamp):
    """获取钱包在指定时间范围内的USDT代币转账"""
    params = {
        'module': 'account',
        'action': 'tokentx',
        'contractaddress': USDT_CONTRACT,
        'address': address,
        'startblock': 0,
        'endblock': 99999999,
        'page': 1,
        'offset': 1000,
        'sort': 'desc',
        'apikey': BSCSCAN_API_KEY
    }
    try:
        response = requests.get(BSCSCAN_API_URL, params=params, timeout=10) # Add timeout
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        if data.get('status') == '1':
            return data.get('result', [])
        else:
            # Log BSCScan API specific errors
            logging.warning(f"BSCScan API returned status {data.get('status')} for address {address}: {data.get('message')}")
            return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching BSCScan data for address {address}: {e}")
        return []
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON response from BSCScan for address {address}.")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred while fetching BSCScan data for address {address}: {e}")
        return []

def get_wallet_token_list(address):
    params = {
        'module': 'account',
        'action': 'tokenlist',
        'address': address,
        'apikey': BSCSCAN_API_KEY
    }
    response = requests.get(BSCSCAN_API_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        if data['status'] == '1':
            return data['result']
    return []

def get_token_prices_coingecko(contract_addresses):
    # contract_addresses: 逗号分隔的合约地址字符串
    url = f'https://api.coingecko.com/api/v3/simple/token_price/binance-smart-chain'
    params = {
        'contract_addresses': contract_addresses,
        'vs_currencies': 'usdt'
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    return {}

def analyze_transactions(transactions, start_timestamp, end_timestamp, address):
    """分析交易数据，统计USDT支出"""
    daily_totals = {}
    
    for tx in transactions:
        # 只处理USDT转出的交易
        if tx['from'].lower() == address.lower() and tx['to'].lower() == USDT_CONTRACT.lower():
            tx_timestamp = int(tx['timeStamp'])
            if start_timestamp <= tx_timestamp <= end_timestamp:
                date = datetime.fromtimestamp(tx_timestamp).strftime('%Y-%m-%d')
                value = float(tx['value']) / 1e18  # 转换为USDT单位
                
                if date not in daily_totals:
                    daily_totals[date] = 0
                daily_totals[date] += value
    
    return daily_totals

def analyze_usdt_to_router(transfers, start_timestamp, end_timestamp, address):
    daily_totals = {}
    for tx in transfers:
        # 只统计USDT从钱包转出到DEX Router的交易
        if tx['from'].lower() == address.lower() and tx['to'].lower() == DEX_ROUTER.lower():
            tx_timestamp = int(tx['timeStamp'])
            if start_timestamp <= tx_timestamp <= end_timestamp:
                date = datetime.utcfromtimestamp(tx_timestamp).strftime('%Y-%m-%d')
                value = float(tx['value']) / 1e18
                if date not in daily_totals:
                    daily_totals[date] = 0
                daily_totals[date] += value
    # 补全15天内无交易的日期
    for i in range(15):
        day = (datetime.utcnow() - timedelta(days=14-i)).strftime('%Y-%m-%d')
        if day not in daily_totals:
            daily_totals[day] = 0
    return dict(sorted(daily_totals.items()))

def send_wx_message(subject, content):
    """Send message via WxPusher"""
    if not WXPUSHER_APP_TOKEN or not WXPUSHER_UID:
        logging.warning("WxPusher APP_TOKEN or UID is not set. Skipping message sending.")
        return

    url = "https://wxpusher.zjiecode.com/api/send/message"
    headers = {
        "Content-Type": "application/json"
    }
    # Convert newlines to HTML breaks first
    content_with_breaks = content.replace('\n', '<br/>')
    # Now use the modified content in the f-string
    html_content = f"<h1>{subject}</h1><br/><p>{content_with_breaks}</p>"
    payload = {
        "appToken": WXPUSHER_APP_TOKEN,
        "content": html_content,
        "summary": subject[:40],  # Limit to 40 characters for summary
        "contentType": 2,  # HTML content type
        "uids": [WXPUSHER_UID],
        "verifyPayType": 0  # Do not verify subscription status
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10) # Add timeout
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        result = response.json()
        if result.get("code") == 1000 and result.get("success"):
            #logging.info(f"Message sent successfully: {result.get('msg')}")
            # Log details for each recipient
            #for record in result.get("data", []):
            #    logging.info(f"Recipient: UID={record.get('uid')}, Status={record.get('status')}, "
            #             f"MessageContentId={record.get('messageContentId')}, SendRecordId={record.get('sendRecordId')}")
            pass # Avoid excessive logging for successful sends
        else:
            logging.warning(f"Failed to send WxPusher message: Code={result.get('code')}, Message={result.get('msg')}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending WxPusher message: {e}")
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON response from WxPusher.")
    except Exception as e:
        logging.error(f"An unexpected error occurred while sending WxPusher message: {e}")

def run_stats():
    logging.info("Starting statistical run...")
    # 获取今天UTC的起止时间
    today = datetime.utcnow().date()
    start_time = datetime(today.year, today.month, today.day)
    end_time = start_time + timedelta(days=1)
    start_timestamp = int(start_time.timestamp())
    end_timestamp = int(end_time.timestamp())

    wallets = load_wallets()
    if not wallets:
        logging.warning("No wallets found in wallets.json. Skipping stats run.")
        return

    # 收集所有钱包的统计信息
    all_wallet_stats_message = ""

    for wallet in wallets:
        address = wallet.get('address')
        alias = wallet.get('alias', 'Unknown Wallet') # Use alias, default to Unknown
        if not address:
             logging.warning(f"Skipping wallet entry with missing address: {wallet}")
             continue

        logging.info(f"Analyzing wallet {alias} ({address})... বৌদ্ধ")
        transfers = get_usdt_token_transfers(address, start_timestamp, end_timestamp)

        # 只统计当天的USDT转出到DEX Router的交易
        count = 0
        total = 0.0
        for tx in transfers:
            # Use .get() for safer dictionary access
            tx_from = tx.get('from', '').lower()
            tx_to = tx.get('to', '').lower()
            tx_timestamp_str = tx.get('timeStamp')
            tx_value_str = tx.get('value')

            if tx_from == address.lower() and tx_to == DEX_ROUTER.lower():
                try:
                    tx_timestamp = int(tx_timestamp_str)
                    # 使用 < end_timestamp 确保只统计到当天的最后一秒
                    if start_timestamp <= tx_timestamp < end_timestamp:
                         value = float(tx_value_str) / 1e18
                         total += value
                         count += 1
                except (ValueError, TypeError) as e:
                     logging.warning(f"Skipping transaction with invalid timestamp or value for wallet {alias}: {tx}. Error: {e}")
                     continue

        logging.info(f"Wallet {alias}: Today\'s stats - {count} purchases, Total {total:.6f} USDT.")

        # 格式化当前钱包的统计信息
        wallet_message = f"钱包 {alias}\n"
        wallet_message += f"今日共进行了{count}笔购买，总共购买了{total:.0f} USDT\n"

        # 计算积分并添加到消息中
        if total >= 2:
            try:
                n = int(math.log(total, 2))
                points_message = f"今天可以获得{n+1}交易积分"
                logging.info(f"Wallet {alias}: {points_message}")
                wallet_message += f"{points_message}\n"
            except ValueError: # Handles total < 1 (log of 0 or negative) - though total >= 2 check prevents this
                 logging.warning(f"Could not calculate points for wallet {alias} with total {total}")
                 wallet_message += f"积分计算失败\n"
        else:
             wallet_message += f"无积分\n"

        # 将当前钱包信息添加到总信息中，并加上分隔符
        all_wallet_stats_message += wallet_message + "\n"

    # 在处理完所有钱包后，发送合并消息
    if all_wallet_stats_message:
        message_subject = "今日BSCScan统计"
        logging.info(f"Sending combined message for all wallets...")
        send_wx_message(message_subject, all_wallet_stats_message)
    else:
        logging.info("No wallet data to send in the combined message.")

    logging.info("Statistical run finished.")

def main():
    if not BSCSCAN_API_KEY:
        logging.error("Error: BSCSCAN_API_KEY is not set in .env file.")
        return

    # 检查WxPusher配置（可选，但建议）
    # if not WXPUSHER_APP_TOKEN or not WXPUSHER_UID:
    #     logging.warning("WxPusher APP_TOKEN or UID is not set in .env file. Messages will not be sent.")

    # 首次启动运行一次
    logging.info("First launch, running stats...")
    run_stats()
    logging.info("First statistical run completed.")

    while True:
        now_utc = datetime.utcnow()
        current_hour_utc = now_utc.hour

        if 0 <= current_hour_utc < 6:
            # 0-6点，每隔1小时运行
            interval_minutes = 60
        elif 6 <= current_hour_utc < 16:
            # 6-16点，每隔半小时运行
            interval_minutes = 30
        else:
            # 16-24点，每隔1小时运行
            interval_minutes = 60

        # 计算下一次运行时间（找到下一个符合间隔的整点或半点）
        current_minute = now_utc.minute
        current_second = now_utc.second
        current_microsecond = now_utc.microsecond

        if interval_minutes == 60:
            # 下一个小时的整点
            if current_minute == 0 and current_second == 0 and current_microsecond == 0:
                # 刚好在整点，立即运行或等待下一个周期？我们选择等待下一个周期
                next_run_time = now_utc + timedelta(hours=1)
                next_run_time = next_run_time.replace(minute=0, second=0, microsecond=0)
            else:
                next_run_time = now_utc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        elif interval_minutes == 30:
            # 下一个半小时的整点或半点
            if 0 <= current_minute < 30:
                 next_run_time = now_utc.replace(minute=30, second=0, microsecond=0)
                 if next_run_time <= now_utc:
                      next_run_time = next_run_time.replace(hour=(now_utc.hour + 1) % 24, minute=0)
                      if next_run_time <= now_utc:
                           next_run_time += timedelta(days=1)
            else:
                 next_run_time = now_utc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else: # Should not happen based on logic, but for safety
             next_run_time = now_utc + timedelta(minutes=interval_minutes)
             logging.warning(f"Unexpected interval_minutes: {interval_minutes}. Falling back to simple timedelta.")

        # 修正：如果计算出的下一次运行时间在当前时间之前（跨天等情况）
        if next_run_time <= now_utc:
             # This case should be handled by the logic above, but as a final check
             next_run_time = now_utc + timedelta(minutes=interval_minutes)
             # For simplicity in this fallback, just go to the next interval from *now*
             logging.warning(f"Calculated next_run_time {next_run_time} is not after now {now_utc}. Recalculating based on interval.")

        sleep_seconds = (next_run_time - now_utc).total_seconds()
        if sleep_seconds < 1: # Ensure at least 1 second sleep
             sleep_seconds = interval_minutes * 60 # Fallback to full interval
             logging.warning(f"Calculated sleep_seconds was too small ({sleep_seconds}). Falling back to full interval sleep: {sleep_seconds}s")

        logging.info(f"Next run at {next_run_time.strftime('%Y-%m-%d %H:%M:%S')} UTC. Sleeping for {int(sleep_seconds)} seconds.")
        time.sleep(sleep_seconds)

        # 休眠结束后运行统计
        logging.info("Resuming stats run...")
        run_stats()
        logging.info("Current run finished.")

if __name__ == "__main__":
    main() 