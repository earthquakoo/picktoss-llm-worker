import os
import pytz
import logging
from datetime import datetime

from core.database.database_manager import DatabaseManager
from core.s3.s3_client import S3Client
from core.discord.discord_client import DiscordClient
from core.llm.openai import OpenAIChatLLM
from core.llm.exception import InvalidLLMJsonResponseError
from core.enums.enum import LLMErrorType, DocumentStatus, QuizType, TransactionType, Source
from core.llm.utils import fill_message_placeholders, load_prompt_messages, markdown_content_splitter


logging.basicConfig(level=logging.INFO)


def multiple_choice_worker(
    s3_client: S3Client,
    discord_client: DiscordClient, 
    chat_llm: OpenAIChatLLM,
    s3_key: str,
    db_pk: int,
    quiz_count: int,
    member_id: int
    ):
    print("Start Multiple choice Worker")
    bucket_obj = s3_client.get_object(key=s3_key)
    content = bucket_obj.decode_content_str()
    
    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])

    # Generate Questions
    content_splits = markdown_content_splitter(content)
        
    # dev & prod
    without_placeholder_messages = load_prompt_messages(
        prompt_path="/var/task/core/llm/prompts/generate_multiple_choice_quiz.txt", quiz_count=quiz_count, placeholder="quiz_count") 
    # local
    # without_placeholder_messages = load_prompt_messages(
    #     prompt_path="core/llm/prompts/generate_multiple_choice_quiz.txt", quiz_count=quiz_count, placeholder="quiz_count")
    total_quiz_count = 0

    timestamp = datetime.now(pytz.timezone('Asia/Seoul'))

    success_at_least_once = False
    failed_at_least_once = False

    for content_split in content_splits:
        print(f"content_split: {content_split}")
        if total_quiz_count >= quiz_count:
            break

        messages = fill_message_placeholders(messages=without_placeholder_messages, placeholders={"note": content_split})
        try:
            resp_dict = chat_llm.predict_json(messages)
            print(resp_dict)
        except InvalidLLMJsonResponseError as e:
            discord_client.report_llm_error(
                task="Question Generation",
                error_type=LLMErrorType.INVALID_JSON_FORMAT,
                document_content=content_split,
                llm_response=e.llm_response,
                error_message="LLM Response is not JSON-decodable",
                info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
            )
            failed_at_least_once = True
            continue
        except Exception as e:
            discord_client.report_llm_error(
                task="Question Generation",
                error_type=LLMErrorType.GENERAL,
                document_content=content_split,
                error_message="Failed to generate questions",
                info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
            )
            failed_at_least_once = True
            continue

        try:
            for q_set in resp_dict:
                if total_quiz_count >= quiz_count:
                    break

                question, answer, options, explanation = q_set["question"], q_set["answer"], q_set["options"], q_set["explanation"]
                correct_answer_count = 0
                delivered_count = 0
                
                quiz_insert_query = "INSERT INTO quiz (question, answer, explanation, delivered_count, quiz_type, correct_answer_count, is_review_needed, is_latest, document_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                
                db_manager.execute_query(quiz_insert_query, (question, answer, explanation, delivered_count, QuizType.MULTIPLE_CHOICE.value, correct_answer_count, False, True, db_pk, timestamp, timestamp))
                quiz_id = db_manager.last_insert_id()
                
                total_quiz_count += 1

                if len(options) == 4:
                    for option in options:
                        option_insert_query = "INSERT INTO options (options, quiz_id, created_at, updated_at) VALUES (%s, %s, %s, %s)"
                        db_manager.execute_query(option_insert_query, (option, quiz_id, timestamp, timestamp))                    
                else:
                    # options가 4개가 아니라면 record에 기록하기
                    continue

        except Exception as e:
            discord_client.report_llm_error(
                task="Question Generation",
                error_type=LLMErrorType.GENERAL,
                document_content=content_split,
                error_message=f"LLM Response is JSON decodable but does not have 'question' and 'answer' keys.\nresp_dict: {resp_dict}",
                info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
            )
            failed_at_least_once = True
            continue

        success_at_least_once = True
    
    if total_quiz_count != quiz_count:
        db_manager.rollback()

        star_select_query = f"SELECT * FROM star WHERE member_id = {member_id}"
        star = db_manager.execute_query(star_select_query)
        cur_star_count = star[0]['star']
        star_id = star[0]['id']

        star_update_query = f"UPDATE star SET star = star + {quiz_count} WHERE member_id = {member_id}"
        star_history_update_query = "INSERT INTO star_history (description, change_amount, balance_after, transaction_type, source, star_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        document_update_query = f"UPDATE document SET quiz_generation_status = 'QUIZ_GENERATION_ERROR' WHERE id = {db_pk}"
        
        db_manager.execute_query(star_update_query)
        db_manager.execute_query(star_history_update_query, ("퀴즈 오류로 인한 별 반환", quiz_count, cur_star_count + quiz_count, TransactionType.DEPOSIT.value, Source.SERVICE.value, star_id, timestamp, timestamp))
        db_manager.execute_query(document_update_query)
        
        db_manager.commit()
        logging.info(f"QUIZ_GENERATION_ERROR")
        return

    
    # Failed at every single generation
    if not success_at_least_once:
        document_update_query = f"UPDATE document SET quiz_generation_status = 'COMPLETELY_FAILED' WHERE id = {db_pk}"
        db_manager.execute_query(document_update_query)
        db_manager.commit()
        logging.info(f"Multiple choice quiz: COMPLETELY_FAILED")
        return

    # Failed at least one chunk question generation
    if failed_at_least_once:
        document_update_query = f"UPDATE document SET quiz_generation_status = 'PARTIAL_SUCCESS' WHERE id = {db_pk}"
        db_manager.execute_query(document_update_query)
        db_manager.commit()
        logging.info(f"Multiple choice quiz: PARTIAL_SUCCESS")

    # ALL successful
    else:
        document_update_query = f"UPDATE document SET quiz_generation_status = 'PROCESSED' WHERE id = {db_pk}"
        db_manager.execute_query(document_update_query)
        db_manager.commit()
        logging.info(f"Multiple choice quiz: PROCESSED")

    db_manager.commit()
    db_manager.close()
    print("End Multiple choice Worker")