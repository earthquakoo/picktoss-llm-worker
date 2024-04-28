from datetime import datetime

from src.core.database.database_manager import DatabaseManager
from src.core.s3.s3_client import S3Client
from src.core.discord.discord_client import DiscordClient
from src.core.llm.openai import OpenAIChatLLM
from src.core.llm.exception import InvalidLLMJsonResponseError
from src.core.enums.enum import LLMErrorType, SubscriptionPlanType, QuizQuestionNum, DocumentStatus, QuizType
from src.core.llm.utils import fill_message_placeholders, load_prompt_messages

def multiple_choice_worker(
    s3_client: S3Client,
    discord_client: DiscordClient, 
    db_manager: DatabaseManager, 
    chat_llm: OpenAIChatLLM,
    s3_key: str,
    db_pk: int,
    subscription_plan: str
    ):
    bucket_obj = s3_client.get_object(key=s3_key)
    content = bucket_obj.decode_content_str()
    
    # Generate Questions
    CHUNK_SIZE = 1100
    chunks: list[str] = []
    for i in range(0, len(content), CHUNK_SIZE):
        chunks.append(content[i : i + CHUNK_SIZE])
        
    # dev & prod
    without_placeholder_messages = load_prompt_messages("/var/task/src/core/llm/prompts/generate_multiple_choice_quiz.txt") 
    # without_placeholder_messages = load_prompt_messages("core/llm/prompts/generate_multiple_choice_quiz.txt") # local
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
                question, answer, options, explanation = q_set["question"], q_set["options"], q_set["answer"], q_set["explanation"]
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
                quiz_insert_query = "INSERT INTO quiz (question, answer, explanation, delivered_count, quiz_type, bookmark, answer_count, document_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                
                timestamp = datetime.now()
                db_manager.execute_query(quiz_insert_query, (question, answer, explanation, delivered_count, QuizType.MULTIPLE_CHOICE.value, False, answer_count, db_pk, timestamp, timestamp))
                quiz_id = db_manager.last_insert_id()
                db_manager.commit()
                
                for option in options:
                    option_insert_query = "INSERT INTO option (option, quiz_id) VALUES (%s, %s)"
                    db_manager.execute_query(option_insert_query, (option, quiz_id))
                
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

    # ALL successful
    else:
        document_update_query = "UPDATE document SET status = %s WHERE id = %s"
        db_manager.execute_query(document_update_query, (DocumentStatus.PROCESSED.value, db_pk))
        db_manager.commit()
        print("PROCESSED")
        
    db_manager.close()