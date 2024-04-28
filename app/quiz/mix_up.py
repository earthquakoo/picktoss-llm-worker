import os
from datetime import datetime

from core.database.database_manager import DatabaseManager
from core.s3.s3_client import S3Client
from core.discord.discord_client import DiscordClient
from core.llm.openai import OpenAIChatLLM
from core.llm.exception import InvalidLLMJsonResponseError
from core.enums.enum import LLMErrorType, SubscriptionPlanType, QuizQuestionNum, DocumentStatus, QuizType
from core.llm.utils import fill_message_placeholders, load_prompt_messages


def mix_up_worker(
    s3_client: S3Client,
    discord_client: DiscordClient, 
    chat_llm: OpenAIChatLLM,
    s3_key: str,
    db_pk: int,
    subscription_plan: str
    ):
    bucket_obj = s3_client.get_object(key=s3_key)
    content = bucket_obj.decode_content_str()
    
    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])
    
    # Generate Questions
    CHUNK_SIZE = 1100
    chunks: list[str] = []
    for i in range(0, len(content), CHUNK_SIZE):
        chunks.append(content[i : i + CHUNK_SIZE])
    # dev & prod
    without_placeholder_messages = load_prompt_messages("/var/task/core/llm/prompts/generate_mix_up_quiz.txt") 
    # local
    # without_placeholder_messages = load_prompt_messages("core/llm/prompts/generate_mix_up_quiz.txt")
    free_plan_question_expose_count = 0
    total_generated_question_count = 0

    success_at_least_once = False
    failed_at_least_once = False

    prev_questions: list[str] = []
    for chunk in chunks:
        prev_question_str = '\n'.join([q for q in prev_questions])
        messages = fill_message_placeholders(messages=without_placeholder_messages, placeholders={"note": chunk, "prev_questions": prev_question_str})
        try:
            resp_dict = chat_llm.predict_json(messages)
        except InvalidLLMJsonResponseError as e:
            discord_client.report_llm_error(
                task="Question Generation",
                error_type=LLMErrorType.INVALID_JSON_FORMAT,
                document_content=chunk,
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
                document_content=chunk,
                error_message="Failed to generate questions",
                info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
            )
            failed_at_least_once = True
            continue

        try:
            for q_set in resp_dict:
                question, answer, explanation = q_set["question"], q_set["answer"], q_set["explanation"]
                answer_count = 0

                # To avoid duplication
                prev_questions.append(question)
                if len(prev_questions) == 6:
                    prev_questions.pop(0)

                total_generated_question_count += 1

                if subscription_plan == SubscriptionPlanType.FREE.value:
                    if free_plan_question_expose_count >= QuizQuestionNum.FREE_PLAN_QUIZ_QUESTION_NUM.value:
                        delivered_count = 0
                    else:
                        delivered_count = 1
                        free_plan_question_expose_count += 1
                elif subscription_plan == SubscriptionPlanType.PRO.value:
                    delivered_count = 1
                else:
                    raise ValueError("Wrong subscription plan type")
                question_insert_query = "INSERT INTO quiz (question, answer, explanation, delivered_count, quiz_type, bookmark, answer_count, document_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                timestamp = datetime.now()
                db_manager.execute_query(question_insert_query, (question, answer, explanation, delivered_count, QuizType.MIX_UP.value, False, answer_count, db_pk, timestamp, timestamp))
                db_manager.commit()

        except Exception as e:
            discord_client.report_llm_error(
                task="Question Generation",
                error_type=LLMErrorType.GENERAL,
                document_content=chunk,
                error_message=f"LLM Response is JSON decodable but does not have 'question' and 'answer' keys.\nresp_dict: {resp_dict}",
                info=f"* s3_key: `{s3_key}`\n* document_id: `{db_pk}`",
            )
            failed_at_least_once = True
            continue

        success_at_least_once = True

    # Failed at every single generation
    if not success_at_least_once:
        document_update_query = "UPDATE document SET status = %s WHERE id = %s"
        db_manager.execute_query(document_update_query, (DocumentStatus.COMPLETELY_FAILED.value, db_pk))
        db_manager.commit()
        print("COMPLETELY_FAILED")
        return

    # Failed at least one chunk question generation
    if failed_at_least_once:
        document_update_query = "UPDATE document SET status = %s WHERE id = %s"
        db_manager.execute_query(document_update_query, (DocumentStatus.PARTIAL_SUCCESS.value, db_pk))
        db_manager.commit()
        print("PARTIAL_SUCCESS")

    else:  # ALL successful
        document_update_query = "UPDATE document SET status = %s WHERE id = %s"
        db_manager.execute_query(document_update_query, (DocumentStatus.PROCESSED.value, db_pk))
        db_manager.commit()
        print("PROCESSED")
        
    db_manager.close()