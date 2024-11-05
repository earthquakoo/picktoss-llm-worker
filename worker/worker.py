import os
import json
import logging

from core.s3.s3_client import S3Client
from core.discord.discord_client import DiscordClient
from core.llm.openai import OpenAIChatLLM
from core.database.database_manager import DatabaseManager
from app.quiz.mix_up import mix_up_worker
from app.quiz.multiple_choice import multiple_choice_worker


logging.basicConfig(level=logging.INFO)

def handler(event, context):
    print(event)
    event_info: str = event["Records"][0]["body"]
    body: dict = json.loads(event_info)
    if "s3_key" not in body or "db_pk" not in body or "subscription_plan" not in body:
        raise ValueError(f"s3_key and db_pk and subscription_plan must be provided. event: {event}, context: {context}")
    
    s3_key = body["s3_key"]
    db_pk = int(body["db_pk"])
    quiz_type = body["quiz_type"]
    quiz_count = body["quiz_count"]

    # core client settings
    s3_client = S3Client(access_key=os.environ["PICKTOSS_AWS_ACCESS_KEY"], secret_key=os.environ["PICKTOSS_AWS_SECRET_KEY"], region_name="us-east-1", bucket_name=os.environ["PICKTOSS_S3_BUCKET_NAME"])
    discord_client = DiscordClient(bot_token=os.environ["PICKTOSS_DISCORD_BOT_TOKEN"], channel_id=os.environ["PICKTOSS_DISCORD_CHANNEL_ID"])
    chat_llm = OpenAIChatLLM(api_key=os.environ["PICKTOSS_OPENAI_API_KEY"], model="gpt-4o-mini")
    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])
    
    get_outbox_query = f"SELECT * FROM outbox WHERE document_id = {db_pk}"
    outbox = db_manager.execute_query(get_outbox_query)

    if not outbox:
        print("There is no data in the outbox table.")
        return {"StatusCode": 200, "message": "There is no data in the outbox table."}

    if outbox[0]['status'] == "PROCESSING":
        print("Data that is already being processed.")
        return {"StatusCode": 200, "message": "Data that is already being processed."}

    if outbox[0]['status'] == "WAITING":
        print("Processing LLM API")
        update_outbox_query = f"UPDATE outbox SET status = 'PROCESSING' WHERE document_id = {db_pk}"
        db_manager.execute_query(update_outbox_query)
        db_manager.commit()

    if quiz_type == "MIX_UP":
        mix_up_worker(s3_client, discord_client, chat_llm, s3_key, db_pk, quiz_count)
    elif quiz_type == "MULTIPLE_CHOICE":
        multiple_choice_worker(s3_client, discord_client, chat_llm, s3_key, db_pk, quiz_count)
    else:
        ## exception
        return 

    delete_outbox_query = f"DELETE FROM outbox WHERE document_id = {db_pk}"
    db_manager.execute_query(delete_outbox_query)
    db_manager.commit()

    return {"statusCode": 200, "message": "hi"}