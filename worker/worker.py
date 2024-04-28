import os
import json
import logging
from threading import Thread

from core.s3.s3_client import S3Client
from core.discord.discord_client import DiscordClient
from core.llm.openai import OpenAIChatLLM
from core.database.database_manager import DatabaseManager
from app.keypoint.keypoint import keypoint_worker
from app.quiz.mix_up import mix_up_worker
from app.quiz.multiple_choice import multiple_choice_worker

logging.basicConfig(level=logging.INFO)


def handler(event, context):
    event_info: str = event["Records"][0]["body"]
    body: dict = json.loads(event_info)
    if "s3_key" not in body or "db_pk" not in body or "subscription_plan" not in body:
        raise ValueError(f"s3_key and db_pk and subscription_plan must be provided. event: {event}, context: {context}")
    
    s3_key = body["s3_key"]
    db_pk = int(body["db_pk"])
    subscription_plan = body["subscription_plan"]
    # core client settings
    s3_client = S3Client(access_key=os.environ["PICKTOSS_AWS_ACCESS_KEY"], secret_key=os.environ["PICKTOSS_AWS_SECRET_KEY"], region_name="us-east-1", bucket_name=os.environ["PICKTOSS_S3_BUCKET_NAME"])
    discord_client = DiscordClient(bot_token=os.environ["PICKTOSS_DISCORD_BOT_TOKEN"], channel_id=os.environ["PICKTOSS_DISCORD_CHANNEL_ID"])
    chat_llm = OpenAIChatLLM(api_key=os.environ["PICKTOSS_OPENAI_API_KEY"], model="gpt-3.5-turbo-0125")
    
    keypoint = Thread(target=keypoint_worker, args=(s3_client, discord_client, chat_llm, s3_key, db_pk, subscription_plan))
    mix_up = Thread(target=mix_up_worker, args=(s3_client, discord_client, chat_llm, s3_key, db_pk, subscription_plan))
    multiple_choice = Thread(target=multiple_choice_worker, args=(s3_client, discord_client, chat_llm, s3_key, db_pk, subscription_plan))
    
    keypoint.start()
    mix_up.start()
    multiple_choice.start()

    return {"statusCode": 200, "message": "hi"}