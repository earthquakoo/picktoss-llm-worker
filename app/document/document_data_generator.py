import os
import pytz
import logging
import time

from core.database.database_manager import DatabaseManager
from core.s3.s3_client import S3Client
from core.discord.discord_client import DiscordClient
from core.llm.openai import OpenAIChatLLM
from core.llm.exception import InvalidLLMJsonResponseError
from core.enums.enum import LLMErrorType
from core.llm.utils import fill_message_placeholders, load_prompt_messages


logging.basicConfig(level=logging.INFO)


def document_data_generator(
    s3_client: S3Client,
    discord_client: DiscordClient, 
    chat_llm: OpenAIChatLLM,
    s3_key: str,
    db_pk: int
    ):
    print("Start Document Data Generation Worker")
    start_time = time.time()
    
    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])

    bucket_obj = s3_client.get_object(key=s3_key)
    content = bucket_obj.decode_content_str()

    language = "en"

    document_select_query = f"SELECT * FROM document WHERE id = {db_pk}"
    document = db_manager.execute_query(document_select_query)
    if document and len(document) > 0:
        language = document[0]['language']

    if language == "en":
        # dev & prod
        prompt_messages = load_prompt_messages(prompt_path="/var/task/core/llm/prompts/generate_en_document_data.txt") 
        # local
        # prompt_messages = load_prompt_messages(prompt_path="core/llm/prompts/generate_en_document_data.txt")
    elif language == "ko":
        # dev & prod
        prompt_messages = load_prompt_messages(prompt_path="/var/task/core/llm/prompts/generate_ko_document_data.txt") 
        # local
        # prompt_messages = load_prompt_messages(prompt_path="core/llm/prompts/generate_document_data.txt")
    else:
        prompt_messages = load_prompt_messages(prompt_path="/var/task/core/llm/prompts/generate_en_document_data.txt")

    messages = fill_message_placeholders(messages=prompt_messages, placeholders={"note": content})

    resp_dict = {'emoji': None, 'title': None, 'category_id': None}

    try:
        resp_dict = chat_llm.predict_json(messages)
        print(f"emoji: {resp_dict['emoji']}")
        print(f"title: {resp_dict['title']}")
        print(f"category_id: {resp_dict['category_id']}")
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
    
    document_emoji = resp_dict['emoji']
    document_title = resp_dict['title']
    document_category = resp_dict['category_id']

    document_update_query = ""

    if document_emoji is None or document_title is None or document_category is None:
        document_update_query = f"UPDATE document SET emoji = NULL, name = NULL, category_id = 6 WHERE id = {db_pk}"
    else:
        document_update_query = f"UPDATE document SET emoji = '{document_emoji}', name = '{document_title}', category_id = {document_category} WHERE id = {db_pk}"
    
    db_manager.execute_query(document_update_query)
    db_manager.commit()

    db_manager.close()
    end_time = time.time()
    print(f"문서 데이터 생성 함수 걸린 시간: {end_time - start_time}")
    print("End Document Data Generation Worker")