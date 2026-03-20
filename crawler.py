#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
异步爬虫（GitHub Actions 版）
抓取 https://www.hanime163.top/watch?v={vid} 的标题、描述、关键词
存储到 SQLite 数据库，支持断点续爬、代理、随机延迟
"""

import os
import asyncio
import json
import logging
import random
import sqlite3
from typing import Dict, Optional

import aiohttp
import aiosqlite
from bs4 import BeautifulSoup

# ==================== 可选 SOCKS5 支持 ====================
try:
    from aiohttp_socks import ProxyConnector
    SOCKS_SUPPORT = True
except ImportError:
    SOCKS_SUPPORT = False
    ProxyConnector = None
    print("提示: 如需使用 SOCKS5 代理，请安装: pip install aiohttp_socks")

# ==================== 配置区域 ====================
BASE_URL = "https://www.hanime163.top/watch?v={}"
VID_RANGE = (1, 405056)             # 可根据需要调整起始和结束
CONCURRENT_TASKS = 3                # 并发数（GitHub Actions 中建议 3~5）
REQUEST_TIMEOUT = 20
RETRY_TIMES = 5
MIN_DELAY = 2
MAX_DELAY = 5
DB_FILE = "hanime_videos.db"        # 数据库文件名

# SOCKS5 代理（通过环境变量配置，如 SOCKS5_PROXY="socks5://user:pass@ip:port"）
SOCKS5_PROXY = os.environ.get("SOCKS5_PROXY", None)

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== 随机 User-Agent ====================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
]

def generate_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Ch-Ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Priority": "u=0, i",
        "Referer": "https://www.hanime163.top/",
    }

# ==================== 数据库初始化（不含 sources 字段） ====================
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS videos (
    vid INTEGER PRIMARY KEY,
    title TEXT,
    description TEXT,
    keywords TEXT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
CREATE_INDEX_SQL = "CREATE INDEX IF NOT EXISTS idx_fetched_at ON videos(fetched_at);"

async def init_db() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(DB_FILE)
    await conn.execute(CREATE_TABLE_SQL)
    await conn.execute(CREATE_INDEX_SQL)
    await conn.commit()
    return conn

async def get_existing_vids(conn: aiosqlite.Connection) -> set:
    async with conn.execute("SELECT vid FROM videos") as cursor:
        rows = await cursor.fetchall()
        return {row[0] for row in rows}

# ==================== HTML 解析（不提取 sources） ====================
def parse_html(html: str, vid: int) -> Optional[Dict]:
    soup = BeautifulSoup(html, 'lxml')

    title_tag = soup.find('title')
    title = title_tag.get_text(strip=True) if title_tag else None

    meta_desc = soup.find('meta', attrs={'name': 'description'})
    description = meta_desc.get('content', '').strip() if meta_desc else None

    meta_keys = soup.find('meta', attrs={'name': 'keywords'})
    keywords = meta_keys.get('content', '').strip() if meta_keys else None

    # 只要有标题或描述之一即认为有效
    if title or description:
        return {
            'vid': vid,
            'title': title,
            'description': description,
            'keywords': keywords,
        }
    else:
        logger.warning(f"VID {vid}: 页面缺少关键信息，可能无效")
        return None

# ==================== 抓取单个 VID（含重试） ====================
async def fetch_vid(session: aiohttp.ClientSession, vid: int, semaphore: asyncio.Semaphore) -> Optional[Dict]:
    url = BASE_URL.format(vid)
    await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    for attempt in range(1, RETRY_TIMES + 1):
        headers = generate_headers()
        try:
            async with semaphore:
                async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        loop = asyncio.get_running_loop()
                        data = await loop.run_in_executor(None, parse_html, html, vid)
                        return data
                    elif resp.status >= 500:
                        wait_time = 5 * (2 ** (attempt - 1))
                        logger.warning(f"VID {vid} 服务器错误 {resp.status}，尝试 {attempt}/{RETRY_TIMES}，等待 {wait_time}秒")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.debug(f"VID {vid} 返回 {resp.status}，跳过")
                        return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug(f"VID {vid} 请求异常: {e}，尝试 {attempt}/{RETRY_TIMES}")
        except Exception as e:
            logger.error(f"VID {vid} 未知错误: {e}，尝试 {attempt}/{RETRY_TIMES}")

        if attempt < RETRY_TIMES:
            await asyncio.sleep(2 ** attempt)

    logger.error(f"VID {vid} 在 {RETRY_TIMES} 次尝试后仍然失败")
    return None

# ==================== 工作协程 ====================
async def worker(queue: asyncio.Queue, session: aiohttp.ClientSession,
                 db_conn: aiosqlite.Connection, semaphore: asyncio.Semaphore, stats: dict):
    while True:
        try:
            vid = await queue.get()
            if vid is None:
                queue.task_done()
                break

            data = await fetch_vid(session, vid, semaphore)
            if data:
                await db_conn.execute(
                    "INSERT OR REPLACE INTO videos (vid, title, description, keywords) VALUES (?, ?, ?, ?)",
                    (data['vid'], data['title'], data['description'], data['keywords'])
                )
                await db_conn.commit()
                stats['succeeded'] += 1
                logger.info(f"成功: VID {vid} (总成功 {stats['succeeded']})")
            else:
                stats['failed'] += 1
                logger.info(f"失败/跳过: VID {vid} (总失败 {stats['failed']})")

            queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Worker 处理 VID {vid} 时发生异常: {e}")
            queue.task_done()

# ==================== 主函数 ====================
async def main():
    db_conn = await init_db()
    existing_vids = await get_existing_vids(db_conn)
    logger.info(f"数据库已有 {len(existing_vids)} 条记录")

    queue = asyncio.Queue()
    total_new = 0
    for vid in range(VID_RANGE[0], VID_RANGE[1] + 1):
        if vid not in existing_vids:
            await queue.put(vid)
            total_new += 1
    logger.info(f"待爬取新 VID 数量: {total_new}")

    if total_new == 0:
        logger.info("没有新 VID 需要处理，退出")
        await db_conn.close()
        return

    semaphore = asyncio.Semaphore(CONCURRENT_TASKS)
    stats = {'succeeded': 0, 'failed': 0}

    # 代理处理
    proxy_url = SOCKS5_PROXY
    if proxy_url and SOCKS_SUPPORT:
        if not proxy_url.startswith(('socks5://', 'socks5h://')):
            proxy_url = 'socks5://' + proxy_url
            logger.info(f"自动补全代理协议头: {proxy_url}")
        else:
            logger.info(f"使用 SOCKS5 代理: {proxy_url}")
        connector = ProxyConnector.from_url(proxy_url, limit=CONCURRENT_TASKS * 2)
    else:
        if proxy_url and not SOCKS_SUPPORT:
            logger.warning("检测到 SOCKS5_PROXY 环境变量，但 aiohttp_socks 未安装，将使用直连")
        logger.info("使用直连（无代理）")
        connector = aiohttp.TCPConnector(limit=CONCURRENT_TASKS * 2, ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [asyncio.create_task(worker(queue, session, db_conn, semaphore, stats))
                   for _ in range(CONCURRENT_TASKS)]

        await queue.join()
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers, return_exceptions=True)

    logger.info(f"爬取完成，成功: {stats['succeeded']}，失败: {stats['failed']}")
    await db_conn.close()

if __name__ == "__main__":
    asyncio.run(main())
