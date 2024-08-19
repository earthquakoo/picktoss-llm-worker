import os
import json
import logging
import time
from threading import Thread

from core.s3.s3_client import S3Client
from core.discord.discord_client import DiscordClient
from core.llm.openai import OpenAIChatLLM
from core.database.database_manager import DatabaseManager
from app.keypoint.keypoint import keypoint_worker
from app.quiz.mix_up import mix_up_worker
from app.quiz.multiple_choice import multiple_choice_worker
from app.quiz.first_today_quiz_set_generator import first_today_quiz_set_generator


logging.basicConfig(level=logging.INFO)


def handler(event, context):
    print(event)
    event_info: str = event["Records"][0]["body"]
    body: dict = json.loads(event_info)
    if "s3_key" not in body or "db_pk" not in body or "subscription_plan" not in body:
        raise ValueError(f"s3_key and db_pk and subscription_plan must be provided. event: {event}, context: {context}")
    
    s3_key = body["s3_key"]
    db_pk = int(body["db_pk"])
    subscription_plan = body["subscription_plan"]
    member_id = int(body["member_id"])
    # core client settings
    s3_client = S3Client(access_key=os.environ["PICKTOSS_AWS_ACCESS_KEY"], secret_key=os.environ["PICKTOSS_AWS_SECRET_KEY"], region_name="us-east-1", bucket_name=os.environ["PICKTOSS_S3_BUCKET_NAME"])
    discord_client = DiscordClient(bot_token=os.environ["PICKTOSS_DISCORD_BOT_TOKEN"], channel_id=os.environ["PICKTOSS_DISCORD_CHANNEL_ID"])
    chat_llm = OpenAIChatLLM(api_key=os.environ["PICKTOSS_OPENAI_API_KEY"], model="gpt-4o-mini")
    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])

    get_outbox_query = f"SELECT * FROM outbox WHERE document_id = {db_pk}"
    outbox: dict = db_manager.execute_query(get_outbox_query)

    if not outbox:
        print("Null outbox")
        return 

    if outbox[0]['status'] == "PROCESSING":
        print("Already processing outbox")
        return 
    
    if outbox[0]['status'] == "WAITING":
        print("Processing LLM API")
        update_outbox_query = f"UPDATE outbox SET status = 'PROCESSING' WHERE document_id = {db_pk}"
        db_manager.execute_query(update_outbox_query)
        db_manager.commit()

    keypoint = Thread(target=keypoint_worker, args=(s3_client, discord_client, chat_llm, s3_key, db_pk, subscription_plan))
    mix_up = Thread(target=mix_up_worker, args=(s3_client, discord_client, chat_llm, s3_key, db_pk, subscription_plan))
    multiple_choice = Thread(target=multiple_choice_worker, args=(s3_client, discord_client, chat_llm, s3_key, db_pk, subscription_plan))
    
    keypoint.start()
    mix_up.start()
    multiple_choice.start()
    
    keypoint.join()
    mix_up.join()
    multiple_choice.join()
    
    first_today_quiz_set_generator(member_id=member_id, db_pk=db_pk)

    delete_outbox_query = f"DELETE FROM outbox WHERE document_id = {db_pk}"
    db_manager.execute_query(delete_outbox_query)
    db_manager.commit()

    return {"statusCode": 200, "message": "hi"}