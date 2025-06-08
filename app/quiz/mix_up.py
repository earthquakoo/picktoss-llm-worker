import os
import time
import pytz
import logging
from datetime import datetime
from langchain_core.runnables import RunnableLambda

from core.s3.s3_client import S3Client
from core.database.database_manager import DatabaseManager
from core.discord.discord_client import DiscordClient
from core.enums.enum import LLMErrorType, QuizType, TransactionType, Source
from core.llm.openai import ChatMessage, OpenAIChatLLM
from core.llm.exception import InvalidLLMJsonResponseError
from core.llm.utils import fill_message_placeholders, load_prompt_messages, content_splitter


logging.basicConfig(level=logging.INFO)


def mix_up_worker(
    s3_client: S3Client,
    discord_client: DiscordClient, 
    chat_llm: OpenAIChatLLM,
    s3_key: str,
    db_pk: int,
    member_id: int,
    star_count: int
    ):
    print("Start Mix-up Worker")
    start_time = time.time()

    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])

    bucket_obj = s3_client.get_object(key=s3_key)
    content = bucket_obj.decode_content_str()
    content_splits = content_splitter(content)

    # dev & prod
    prompt_messages = load_prompt_messages(prompt_path="/var/task/core/llm/prompts/generate_mix_up_quiz.txt") 
    # local
    # prompt_messages = load_prompt_messages("core/llm/prompts/generate_mix_up_quiz.txt")

    batch_inputs: list[list[ChatMessage]] = []
    for split in content_splits:
        filled_messages = fill_message_placeholders(
            messages=prompt_messages,
            placeholders={"note": split}
        )
        batch_inputs.append(filled_messages)

    predict_json_runnable = RunnableLambda(chat_llm.predict_json)
    results = []

    success_at_least_once = False
    failed_at_least_once = False

    try:
        results = predict_json_runnable.batch(batch_inputs)
    except InvalidLLMJsonResponseError as e:
        discord_client.report_llm_error(
            task="Question Generation",
            error_type=LLMErrorType.INVALID_JSON_FORMAT,
            document_content=content_splits[0],
            llm_response=e.llm_response,
            error_message="LLM Response is not JSON-decodable",
            info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
        )
        failed_at_least_once = True
    except Exception as e:
        discord_client.report_llm_error(
            task="Question Generation",
            error_type=LLMErrorType.GENERAL,
            document_content=content_splits[0],
            error_message="Failed to generate questions",
            info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
        )
        failed_at_least_once = True

    timestamp = datetime.now(pytz.timezone('Asia/Seoul'))

    total_quiz_count = 0


    for i, result in enumerate(results):
        print(f"Chunk {i + 1} result:", result)

        try:
            for q_set in result:

                question, answer, explanation = q_set["question"], q_set["answer"], q_set["explanation"]
                correct_answer_count = 0
                delivered_count = 0
                
                question = question.replace("(True/False)", "").strip()
                question = question.replace("(O/X)", "").strip()
                
                if answer == "incorrect" or answer == "correct":
                    question_insert_query = "INSERT INTO quiz (question, answer, explanation, delivered_count, quiz_type, correct_answer_count, is_review_needed, is_latest, document_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    db_manager.execute_query(question_insert_query, (question, answer, explanation, delivered_count, QuizType.MIX_UP.value, correct_answer_count, False, True, db_pk, timestamp, timestamp))
                else:
                    print("정답 오류")
                    continue

                total_quiz_count += 1

        except Exception as e:
            discord_client.report_llm_error(
                task="Question Generation",
                error_type=LLMErrorType.GENERAL,
                document_content=result[i],
                error_message=f"LLM Response is JSON decodable but does not have 'question' and 'answer' keys.\nresp_dict: {result}",
                info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
            )
            failed_at_least_once = True
            continue

        success_at_least_once = True

    db_manager.commit()

    # Failed at every single generation
    if not success_at_least_once or total_quiz_count <= 5:
        db_manager.rollback()

        star_select_query = f"SELECT * FROM star WHERE member_id = {member_id}"
        star = db_manager.execute_query(star_select_query)
        cur_star_count = star[0]['star']
        star_id = star[0]['id']

        star_update_query = f"UPDATE star SET star = star + {star_count} WHERE member_id = {member_id}"
        star_history_update_query = "INSERT INTO star_history (description, change_amount, balance_after, transaction_type, source, star_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        document_status_update_query = f"UPDATE document SET quiz_generation_status = 'QUIZ_GENERATION_ERROR' WHERE id = {db_pk}"
        documnet_is_public_update_query = f"UPDATE document SET is_public = false WHERE id = {db_pk}"
        
        db_manager.execute_query(star_update_query)
        db_manager.execute_query(star_history_update_query, ("퀴즈 오류로 인한 별 반환", star_count, cur_star_count + star_count, TransactionType.DEPOSIT.value, Source.SERVICE.value, star_id, timestamp, timestamp))
        db_manager.execute_query(document_status_update_query)
        db_manager.execute_query(documnet_is_public_update_query)
        
        db_manager.commit()
        logging.info(f"QUIZ_GENERATION_ERROR")
        return

    # Failed at least one chunk question generation
    if failed_at_least_once:
        document_status_update_query = f"UPDATE document SET quiz_generation_status = 'PARTIAL_SUCCESS' WHERE id = {db_pk}"
        db_manager.execute_query(document_status_update_query)
        db_manager.commit()
        logging.info(f"MixUp quiz: PARTIAL_SUCCESS")

    else:  # ALL successful
        document_status_update_query = f"UPDATE document SET quiz_generation_status = 'PROCESSED' WHERE id = {db_pk}"
        db_manager.execute_query(document_status_update_query)
        db_manager.commit()
        logging.info(f"MixUp quiz: PROCESSED")
    
    
    db_manager.close()
    end_time = time.time()
    print(f"퀴즈 생성 함수 걸린 시간: {end_time - start_time}")
    print("End Mix-up Worker")