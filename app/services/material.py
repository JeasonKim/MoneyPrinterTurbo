import os
import random
import aiohttp
import asyncio
from urllib.parse import urlencode
import ffmpeg
import subprocess

import requests
from typing import List
from loguru import logger
from moviepy.video.io.VideoFileClip import VideoFileClip

from app.config import config
from app.models.schema import VideoAspect, VideoConcatMode, MaterialInfo
from app.utils import utils

requested_count = 0
download_concurreny_num = asyncio.Semaphore(15)


def get_api_key(cfg_key: str):
    api_keys = config.app.get(cfg_key)
    if not api_keys:
        raise ValueError(
            f"\n\n##### {cfg_key} is not set #####\n\nPlease set it in the config.toml file: {config.config_file}\n\n"
            f"{utils.to_json(config.app)}"
        )

    # if only one key is provided, return it
    if isinstance(api_keys, str):
        return api_keys

    global requested_count
    requested_count += 1
    return api_keys[requested_count % len(api_keys)]


def search_videos_pexels(
        search_term: str,
        minimum_duration: int,
        video_aspect: VideoAspect = VideoAspect.portrait,
) -> List[MaterialInfo]:
    aspect = VideoAspect(video_aspect)
    video_orientation = aspect.name
    video_width, video_height = aspect.to_resolution()
    api_key = get_api_key("pexels_api_keys")
    headers = {"Authorization": api_key, "User-Agent": "Mozilla/5.0"}
    # Build URL
    params = {"query": search_term, "per_page": 20, "orientation": video_orientation}
    query_url = f"https://api.pexels.com/videos/search?{urlencode(params)}"
    logger.info(f"searching videos: {query_url}, with proxies: {config.proxy}")

    try:
        r = requests.get(
            query_url,
            headers=headers,
            proxies=config.proxy,
            verify=False,
            timeout=(30, 60),
        )
        response = r.json()
        video_items = []
        if "videos" not in response:
            logger.error(f"search videos failed: {response}")
            return video_items
        videos = response["videos"]
        # loop through each video in the result
        for v in videos:
            duration = v["duration"]
            # check if video has desired minimum duration
            if duration < minimum_duration:
                continue
            video_files = v["video_files"]
            # loop through each url to determine the best quality
            for video in video_files:
                w = int(video["width"])
                h = int(video["height"])
                if w == video_width and h == video_height:
                    item = MaterialInfo()
                    item.provider = "pexels"
                    item.url = video["link"]
                    item.duration = duration
                    video_items.append(item)
                    break
        return video_items
    except Exception as e:
        logger.error(f"search videos failed: {str(e)}")

    return []


def search_videos_pixabay(
        search_term: str,
        minimum_duration: int,
        video_aspect: VideoAspect = VideoAspect.portrait,
) -> List[MaterialInfo]:
    aspect = VideoAspect(video_aspect)

    video_width, video_height = aspect.to_resolution()

    api_key = get_api_key("pixabay_api_keys")
    # Build URL
    params = {
        "q": search_term,
        "video_type": "all",  # Accepted values: "all", "film", "animation"
        "per_page": 50,
        "key": api_key,
    }
    query_url = f"https://pixabay.com/api/videos/?{urlencode(params)}"
    logger.info(f"searching videos: {query_url}, with proxies: {config.proxy}")

    try:
        r = requests.get(
            query_url, proxies=config.proxy, verify=False, timeout=(30, 60)
        )
        response = r.json()
        video_items = []
        if "hits" not in response:
            logger.error(f"search videos failed: {response}")
            return video_items
        videos = response["hits"]
        # loop through each video in the result
        for v in videos:
            duration = v["duration"]
            # check if video has desired minimum duration
            if duration < minimum_duration:
                continue
            video_files = v["videos"]
            # loop through each url to determine the best quality
            for video_type in dict(reversed(video_files.items())):
                video = video_files[video_type]
                w = int(video["width"])
                h = int(video["height"])
                if w >= video_width:
                    item = MaterialInfo()
                    item.provider = "pixabay"
                    item.url = video["url"]
                    item.duration = duration
                    video_items.append(item)
                    break
        return video_items
    except Exception as e:
        logger.error(f"search videos failed: {str(e)}")

    return []


async def save_video(video_url: str, save_dir: str = "", retries: int = 3) -> str:
    if not save_dir:
        save_dir = utils.storage_dir("cache_videos")

    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    url_without_query = video_url.split("?")[0]
    url_hash = utils.md5(url_without_query)
    video_id = f"vid-{url_hash}"
    video_path = f"{save_dir}/{video_id}.mp4"

    # if video already exists, return the path
    if os.path.exists(video_path) and os.path.getsize(video_path) > 0:
        logger.info(f"video already exists: {video_path}")
        return video_path
    # Download the video asynchronously
    async with download_concurreny_num:
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            video_url,
                            headers={"User-Agent": "Mozilla/5.0"},
                            proxy=config.proxy['http'],
                            ssl=False,
                            timeout=aiohttp.ClientTimeout(total=5*60)
                    ) as response:
                        if response.status == 200:
                            video_size = response.content_length
                            logger.info(f"videoId: {video_id}, url: {video_url}")
                            with open(video_path, 'wb') as f:
                                while True:
                                    chunk = await response.content.read(1024)
                                    if not chunk:
                                        break
                                    f.write(chunk)
                        else:
                            logger.error(f"Failed to download videoId: {video_id}, url: {video_url}，error code: {response.status}")
                            return ""
            except aiohttp.ClientPayloadError as e:
                logger.warning(f"Download interrupt，retry {attempt + 1}/{retries} times: {str(e)}")
                await asyncio.sleep(0.2)  # Wait 1 second and try again
                continue
            except Exception as e:
                logger.error(f"Failed to download videoId: {video_id}, url: {video_url}: {str(e)}")
                if os.path.exists(video_path):
                    os.remove(video_path)
                return ""
            else:
                break
        else:
            logger.error("Failed to download videoId: {video_id}, url: {video_url} : Download failed after multiple retries")
            if os.path.exists(video_path):
                os.remove(video_path)
            return ""

    # Verify video integrity
    if check_video_integrity(video_path, video_size):
        return video_path
    else:
        if os.path.exists(video_path):
            os.remove(video_path)
        logger.warning(f"Invalid video file: {video_path}")

    return ""


async def download_videos(
        task_id: str,
        search_terms: List[str],
        source: str = "pexels",
        video_aspect: VideoAspect = VideoAspect.portrait,
        video_contact_mode: VideoConcatMode = VideoConcatMode.random,
        audio_duration: float = 0.0,
        max_clip_duration: int = 5,
) -> List[str]:
    valid_video_urls = []
    search_videos = search_videos_pexels if source == "pexels" else search_videos_pixabay

    for search_term in search_terms:
        video_items = search_videos(
            search_term=search_term,
            minimum_duration=max_clip_duration,
            video_aspect=video_aspect,
        )
        logger.info(f"Found {len(video_items)} videos for '{search_term}'")
        valid_video_urls.extend([item.url for item in video_items])

    if video_contact_mode == "random":
        random.shuffle(valid_video_urls)

    material_directory = config.app.get("material_directory", "").strip()
    if material_directory == "task":
        material_directory = utils.task_dir(task_id)
    elif material_directory and not os.path.isdir(material_directory):
        material_directory = ""

    total_duration = 0.0
    result = []

    # create tasks
    tasks = [save_video(url, save_dir=material_directory) for url in valid_video_urls]
    video_paths = await asyncio.gather(*tasks)  # Run all tasks concurrently
    for video_path in video_paths:
        if video_path:
            result.append(video_path)
            with VideoFileClip(video_path) as clip:
                if clip.duration < max_clip_duration:
                    continue
                total_duration += min(max_clip_duration, clip.duration)
            if total_duration >= audio_duration:
                break

    logger.success(f"Downloaded {len(result)} videos")
    return result


def check_video_integrity(file_path: str, video_size: int) -> bool:
    if os.path.getsize(file_path) != video_size:
        return False
    try:
        # 构建等效于 "ffmpeg -v error -i <file> -f null -" 的命令
        process = (
            ffmpeg
            .input(file_path)  # 指定输入文件
            .output('null', f='null')  # 指定输出格式为 null
            .global_args('-v', 'error')  # 仅输出错误信息
        )

        # 使用 subprocess.run 来执行 ffmpeg 命令并捕获错误输出
        result = subprocess.run(
            process.compile(),
            stderr=subprocess.PIPE,
            universal_newlines=True
        )

        # 检查命令是否成功执行，没有错误返回 True
        if result.returncode == 0 and result.stderr == '':
            return True
        else:
            return False

    except subprocess.CalledProcessError as e:
        print(f"检测过程中发生错误：{e}")
        return False


if __name__ == "__main__":
    # asyncio.run(save_video("https://videos.pexels.com/video-files/13433115/13433115-hd_1080_1920_24fps.mp4"))
    asyncio.run(download_videos(
        "test123", ["loan risks", "stock market borrowing", "legal promissory note",
                    "financial responsibility", "investment caution"], audio_duration=10, source="pixabay"
    ))
