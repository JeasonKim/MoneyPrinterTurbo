import math
import random
import asyncio
import os.path
import re
from os import path

from loguru import logger

from app.config import config
from app.models import const
from app.models.schema import VideoConcatMode, VideoParams, MaterialInfo, Task
from app.services import llm, material, subtitle, video, voice
from app.services import state as sm
from app.utils import utils


def generate_script(task_id: str, task: Task):
    params = task.params
    logger.info("\n\n## generating video script")
    video_script = params.video_script.strip()
    if not video_script:
        video_script = llm.generate_script(
            video_subject=params.video_subject,
            language=params.video_language,
            paragraph_number=params.paragraph_number,
        )
    else:
        logger.debug(f"video script: \n{video_script}")

    if not video_script:
        sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)
        logger.error("failed to generate video script.")
    else:
        task.script = video_script


def generate_terms(task_id: str, task: Task):
    logger.info("\n\n## generating video terms")
    params = task.params
    video_terms = params.video_terms
    if not video_terms:
        video_terms = llm.generate_terms(
            video_subject=params.video_subject, video_script=task.script.replace(const.AD_INSERT_IDENTIFIER, ""),
            amount=5
        )
    else:
        if isinstance(video_terms, str):
            video_terms = [term.strip() for term in re.split(r"[,，]", video_terms)]
        elif isinstance(video_terms, list):
            video_terms = [term.strip() for term in video_terms]
        else:
            raise ValueError("video_terms must be a string or a list of strings.")

        logger.debug(f"video terms: {utils.to_json(video_terms)}")

    if not video_terms:
        sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)
        logger.error("failed to generate video terms.")
    else:
        task.search_terms = video_terms


def save_script_data(task_id: str, task: Task):
    with open(utils.task_script_file(task_id), "w", encoding="utf-8") as f:
        f.write(utils.to_json(task.dict()))


def is_ad_enable(params: VideoParams):
    return const.AD_INSERT_IDENTIFIER in params.video_script and params.ad_enabled


def insert_ad_script(script: str, ad_script: str):
    return script.replace(const.AD_INSERT_IDENTIFIER, ad_script)


def ad_split_script(script: str):
    return script.split(const.AD_INSERT_IDENTIFIER)


def correct_script(script: str):
    return script.replace(const.AD_INSERT_IDENTIFIER, "")


def generate_audio(task_id: str, task: Task):
    video_script = task.script
    params = task.params
    subtitle_enabled = task.params.subtitle_enabled
    if is_ad_enable(params):
        logger.info("\n\n## generating audio with ad")
        pre_script, post_script = ad_split_script(video_script)
        ad_sub_maker = voice.tts(
            text=params.ad_script,
            voice_name=voice.parse_voice_name(params.voice_name),
            voice_rate=params.voice_rate,
            voice_file=utils.task_ad_audio_file(task_id),
        )
        task.ad_duration = math.ceil(voice.get_audio_duration(ad_sub_maker))
        generate_subtitle(subtitle_enabled, params.ad_script, utils.task_ad_subtitle_file(task_id), ad_sub_maker)

        pre_sub_maker = voice.tts(
            text=pre_script,
            voice_name=voice.parse_voice_name(params.voice_name),
            voice_rate=params.voice_rate,
            voice_file=utils.task_pre_audio_file(task_id),
        )
        task.pre_ad_duration = math.ceil(voice.get_audio_duration(pre_sub_maker))
        generate_subtitle(subtitle_enabled, pre_script, utils.task_pre_subtitle_file(task_id), pre_sub_maker)

        post_sub_maker = voice.tts(
            text=post_script,
            voice_name=voice.parse_voice_name(params.voice_name),
            voice_rate=params.voice_rate,
            voice_file=utils.task_post_audio_file(task_id),
        )
        task.post_ad_duration = math.ceil(voice.get_audio_duration(post_sub_maker))
        generate_subtitle(subtitle_enabled, post_script, utils.task_post_subtitle_file(task_id), post_sub_maker)
        task.total_duration = task.pre_ad_duration + task.ad_duration + task.post_ad_duration
    else:
        logger.info("\n\n## generating audio without ad")
        correct_video_script = correct_script(video_script)
        sub_maker = voice.tts(
            text=correct_video_script,
            voice_name=voice.parse_voice_name(params.voice_name),
            voice_rate=params.voice_rate,
            voice_file=utils.task_audio_file(task_id),
        )
        if sub_maker is None:
            sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)
            logger.error(
                """failed to generate audio:
                    1. check if the language of the voice matches the language of the video script.
                    2. check if the network is available. If you are in China, it is recommended to use a VPN and enable the global traffic mode.
                """.strip()
            )
        else:
            task.total_duration = math.ceil(voice.get_audio_duration(sub_maker))
            generate_subtitle(subtitle_enabled, correct_video_script, utils.task_subtitle_file(task_id), sub_maker)


def generate_subtitle(subtitle_enabled: bool, script: str, path: str, sub_maker):
    if not subtitle_enabled:
        return ""

    subtitle_provider = config.app.get("subtitle_provider", "").strip().lower()
    logger.info(f"\n\n## generating subtitle, provider: {subtitle_provider}")

    subtitle_fallback = False
    if subtitle_provider == "edge":
        voice.create_subtitle(
            text=script, sub_maker=sub_maker, subtitle_file=path
        )
        if not os.path.exists(path):
            subtitle_fallback = True
            logger.warning("subtitle file not found, fallback to whisper")

    # if subtitle_provider == "whisper" or subtitle_fallback:
    #     subtitle.create(audio_file=utils.task_audio_file(task_id), subtitle_file=path)
    #     logger.info("\n\n## correcting subtitle")
    #     subtitle.correct(subtitle_file=path, video_script=script)

    subtitle_lines = subtitle.file_to_subtitles(path)
    if not subtitle_lines:
        logger.warning(f"subtitle file is invalid: {path}")


def get_video_materials(task_id: str, task: Task):
    params = task.params
    if is_ad_enable(params):
        ad_material_info = MaterialInfo()
        ad_material_info.provider = "local"
        ad_material_info.url = params.ad_url
        ad_material_info.search_term = "ad"
        ad_material_info.duration = task.ad_duration
        ad_materials = video.preprocess_video(
            materials=[ad_material_info], clip_duration=task.ad_duration
        )
        task.ad_material_info = ad_materials[0]
    if params.video_source == "local":
        logger.info("\n\n## preprocess local materials")
        materials = video.preprocess_video(
            materials=params.video_materials, clip_duration=params.video_clip_duration
        )
        if not materials:
            sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)
            logger.error(
                "no valid materials found, please check the materials and try again."
            )
            return None
        return [material_info.url for material_info in materials]
    else:
        logger.info(f"\n\n## downloading videos from {params.video_source}")
        downloaded_videos = asyncio.run(material.download_videos(
            task_id=task_id,
            search_terms=task.search_terms,
            source=params.video_source,
            video_aspect=params.video_aspect,
            video_contact_mode=params.video_concat_mode,
            audio_duration=task.total_duration * params.video_count,
            max_clip_duration=params.video_clip_duration,
        ))
        if not downloaded_videos:
            sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)
            logger.error(
                "failed to download videos, maybe the network is not available. if you are in China, please use a VPN."
            )
            return None
        task.material_urls = downloaded_videos


def generate_final_videos(task_id: str, task: Task):
    params = task.params
    final_video_paths = []
    combined_video_paths = []
    video_concat_mode = (
        params.video_concat_mode if params.video_count == 1 else VideoConcatMode.random
    )
    final_ad_video_path = path.join(utils.task_dir(task_id), f"final-ad.mp4")
    video.generate_video(
        video_path=task.ad_material_info.url,
        audio_path=utils.task_ad_audio_file(task_id),
        subtitle_path=utils.task_ad_subtitle_file(task_id),
        output_file=final_ad_video_path,
        params=params,
    )
    _progress = 50
    for i in range(params.video_count):
        index = i + 1
        combined_video_path = path.join(
            utils.task_dir(task_id), f"combined-{index}.mp4"
        )
        logger.info(f"\n\n## combining video: {index} => {combined_video_path}")
        video.combine_videos(
            combined_video_path=combined_video_path,
            video_paths=task.material_urls,
            audio_file=utils.task_audio_file(task_id),
            video_aspect=params.video_aspect,
            video_concat_mode=video_concat_mode,
            max_clip_duration=params.video_clip_duration,
            threads=params.n_threads,
        )

        _progress += 50 / params.video_count / 2
        sm.state.update_task(task_id, progress=_progress)

        final_video_path = path.join(utils.task_dir(task_id), f"final-{index}.mp4")

        logger.info(f"\n\n## generating video: {index} => {final_video_path}")
        video.generate_video(
            video_path=combined_video_path,
            audio_path=utils.task_audio_file(task_id),
            subtitle_path=utils.task_subtitle_file(task_id),
            output_file=final_video_path,
            params=params,
        )

        _progress += 50 / params.video_count / 2
        sm.state.update_task(task_id, progress=_progress)

        final_video_paths.append(final_video_path)
        combined_video_paths.append(combined_video_path)

    return final_video_paths, combined_video_paths


def start(task_id, params: VideoParams, stop_at: str = "video"):
    logger.info(f"start task: {task_id}, stop_at: {stop_at}")
    sm.state.update_task(task_id, state=const.TASK_STATE_PROCESSING, progress=5)

    if type(params.video_concat_mode) is str:
        params.video_concat_mode = VideoConcatMode(params.video_concat_mode)

    task = Task(params=params)

    # 1. Generate script
    generate_script(task_id, task)
    video_script = task.script
    if not video_script:
        sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)
        return

    sm.state.update_task(task_id, state=const.TASK_STATE_PROCESSING, progress=10)

    if stop_at == "script":
        sm.state.update_task(
            task_id, state=const.TASK_STATE_COMPLETE, progress=100, script=video_script
        )
        return {"script": video_script}

    # 2. Generate terms
    if params.video_source != "local":
        generate_terms(task_id, task)
        video_terms = task.search_terms
        if not video_terms:
            sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)
            return

    save_script_data(task_id, task)

    if stop_at == "terms":
        sm.state.update_task(
            task_id, state=const.TASK_STATE_COMPLETE, progress=100, terms=video_terms
        )
        return {"script": video_script, "terms": video_terms}

    sm.state.update_task(task_id, state=const.TASK_STATE_PROCESSING, progress=20)

    # 3. Generate audio
    generate_audio(task_id, task)
    audio_file = utils.task_audio_file(task_id),
    if not audio_file:
        sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)
        return

    sm.state.update_task(task_id, state=const.TASK_STATE_PROCESSING, progress=30)

    if stop_at == "audio":
        sm.state.update_task(
            task_id,
            state=const.TASK_STATE_COMPLETE,
            progress=100,
            audio_file=audio_file,
            ad_audio_file=utils.task_ad_audio_file(task_id),
            pre_audio_file=utils.task_pre_audio_file(task_id),
            post_audio_file=utils.task_post_audio_file(task_id),
        )
        return {
            "audio_file": audio_file,
            "audio_duration": task.total_duration,
            "ad_audio_file": utils.task_ad_audio_file(task_id),
            "ad_audio_duration": task.ad_duration,
            "pre_audio_file": utils.task_pre_audio_file(task_id),
            "pre_audio_duration": task.pre_ad_duration,
            "post_audio_file": utils.task_post_audio_file(task_id),
            "post_audio_duration": task.post_ad_duration,
        }

    # 5. Get video materials
    get_video_materials(task_id, task)
    downloaded_videos = task.material_urls
    if not downloaded_videos:
        sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)
        return

    if stop_at == "materials":
        sm.state.update_task(
            task_id,
            state=const.TASK_STATE_COMPLETE,
            progress=100,
            materials=downloaded_videos,
        )
        return {"materials": downloaded_videos}

    sm.state.update_task(task_id, state=const.TASK_STATE_PROCESSING, progress=50)

    # 6. Generate final videos
    final_video_paths, combined_video_paths = generate_final_videos(task_id, task)

    if not final_video_paths:
        sm.state.update_task(task_id, state=const.TASK_STATE_FAILED)
        return

    logger.success(
        f"task {task_id} finished, generated {len(final_video_paths)} videos."
    )

    kwargs = {
        "videos": final_video_paths,
        "combined_videos": combined_video_paths,
        "script": video_script,
        "terms": video_terms,
        "audio_file": audio_file,
        "audio_duration": task.total_duration,
        "subtitle_path": utils.task_subtitle_file(task_id),
        "materials": downloaded_videos,
    }
    sm.state.update_task(
        task_id, state=const.TASK_STATE_COMPLETE, progress=100, **kwargs
    )
    return kwargs


def test_remote():
    params = VideoParams(
        ad_enabled=True,
        ad_script="在这里我给大家推荐一款好用的工具，让大家不再困扰。",
        video_subject="描述借钱不还的窘境，推荐用户通过法律服务追回欠款",
        video_script="借钱不还的情况往往让出借人陷入极大的困扰和无奈，既不愿破坏关系，又担心难以追回款项，造成财务压力。长期追讨欠款可能让人身心疲惫，甚至导致人际关系恶化。面对这种窘境，依靠法律服务是一种稳妥且有效的解决方式。@@通过法律途径，不仅可以凭借借条等证据保障自己的合法权益，还能避免不必要的纠纷和矛盾，使讨债过程更加规范和有力，最终帮助你及时追回欠款，减轻经济负担。",
        video_terms="unpaid debt issue, financial stress, legal recovery, protect rights, debt collection assistance",
        voice_name="zh-CN-YunyangNeural-Male",
        voice_rate=1.2,
        video_count=1
    )
    start(params.video_subject, params, stop_at="subtitle")


def test_local():
    video_materials = []
    with os.scandir("/Users/jinjianxun/code/llm/MoneyPrinterTurbo/storage/cache_videos") as entries:
        files = [entry for entry in entries if
                 entry.is_file() and (entry.name.endswith(".mp4") or entry.name.endswith(".png"))]
        for _ in range(10):
            # 随机选择一个元素
            element = random.choice(files)
            # 从数组中移除已经选择的元素
            files.remove(element)
            video_materials.append(MaterialInfo(url=element.path))

    params = VideoParams(
        video_source="local",
        video_materials=video_materials,
        video_subject="主动打借条更容易借到钱111",
        video_script="主动打借条可以增强借款人对出借人的信任感，让对方觉得你是一个有责任心且值得信赖的人，从而更容易获得对方的支持。借条明确了借款的金额、期限和还款条件，这不仅保障了出借人的权益，还表明你对借款的认真态度。通过主动提出签署借条，你传达了对还款的承诺与尊重，减少了出借人的顾虑。这样既有助于建立互信，又提升了借款成功的可能性，最终使双方都感到安心和有保障。",
        video_terms="build trust, responsible borrower, loan agreement benefits, enhance credibility, increase loan approval",
        voice_name="zh-CN-YunyangNeural-Male",
        voice_rate=1.2,
        video_count=1
    )
    start(params.video_subject, params, stop_at="video")


def test_ad():
    params = VideoParams(
        video_subject="主动打借条更容易借到钱111",
        video_script="主动打借条可以增强借款人对出借人的信任感，让对方觉得你是一个有责任心且值得信赖的人，从而更容易获得对方的支持。@@借条明确了借款的金额、期限和还款条件，这不仅保障了出借人的权益，还表明你对借款的认真态度。通过主动提出签署借条，你传达了对还款的承诺与尊重，减少了出借人的顾虑。这样既有助于建立互信，又提升了借款成功的可能性，最终使双方都感到安心和有保障。",
        video_terms="build trust, responsible borrower, loan agreement benefits, enhance credibility, increase loan approval",
        voice_name="zh-CN-YunyangNeural-Male",
        voice_rate=1.2,
        video_count=1,
        ad_enabled=True,
        ad_url="/Users/jinjianxun/电子借条.png",
        ad_script="我给大家推荐一款好用的借条工具"
    )
    start(params.video_subject, params, stop_at="video")


if __name__ == "__main__":
    test_ad()
