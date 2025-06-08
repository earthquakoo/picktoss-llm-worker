import os
import pytz
import logging
import time
from datetime import datetime

from core.database.database_manager import DatabaseManager
from core.s3.s3_client import S3Client
from core.discord.discord_client import DiscordClient
from core.llm.openai import OpenAIChatLLM
from core.llm.exception import InvalidLLMJsonResponseError
from core.enums.enum import LLMErrorType, DocumentStatus, QuizType, TransactionType, Source
from core.llm.utils import fill_message_placeholders, load_prompt_messages, content_splitter


logging.basicConfig(level=logging.INFO)

# 전체 문서를 기반한 test worker
# 전체 문서를 기반해서 생성하더라도 cold start가 아닐 때는 로컬에서 3~4초정도 소요됨
# 아마 생성해야하는 것이 제목뿐만이고 요구사항 등이 적어서 빠르게 생성할 수 있는 것 같다.

# 해야할 것
# 문서의 제목이 생성안되었을 경우 예외처리를 어떻게 할 것이며 그 이후 동작들은 어떻게 처리할 것인지
# 문서 제목 설정 코스트 얼마나 발생하는지 확인하기
# 퀴즈 생성 시간 단축시켜보기

def title_generation_worker(
    s3_client: S3Client,
    discord_client: DiscordClient, 
    chat_llm: OpenAIChatLLM,
    s3_key: str,
    db_pk: int
    ):
    print("Start Title Generation Worker")
    start_time = time.time()
    
    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])

    bucket_obj = s3_client.get_object(key=s3_key)
    content = bucket_obj.decode_content_str()

    # dev & prod
    prompt_messages = load_prompt_messages(prompt_path="/var/task/core/llm/prompts/generate_title.txt") 
    # local
    # prompt_messages = load_prompt_messages(prompt_path="core/llm/prompts/generate_title.txt")

    messages = fill_message_placeholders(messages=prompt_messages, placeholders={"note": content})

    resp_dict = {'title': None}

    try:
        resp_dict = chat_llm.predict_json(messages)
        print(f"title: {resp_dict['title']}")
    except InvalidLLMJsonResponseError as e:
        discord_client.report_llm_error(
            task="Question Generation",
            error_type=LLMErrorType.INVALID_JSON_FORMAT,
            document_content=content,
            llm_response=e.llm_response,
            error_message="LLM Response is not JSON-decodable",
            info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
        )
    except Exception as e:
        discord_client.report_llm_error(
            task="Question Generation",
            error_type=LLMErrorType.GENERAL,
            document_content=content,
            error_message="Failed to generate questions",
            info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
        )
    
    document_title = resp_dict['title']

    document_title_update_query = ""
    if document_title is None:
        document_title_update_query = f"UPDATE document SET name = NULL where id = {db_pk}"
    else:
        document_title_update_query = f"UPDATE document SET name = '{document_title}' where id = {db_pk}"
    
    db_manager.execute_query(document_title_update_query)
    db_manager.commit()


    db_manager.close()
    end_time = time.time()
    print(f"제목 생성 함수 걸린 시간: {end_time - start_time}")
    print("End Title Generation Worker")