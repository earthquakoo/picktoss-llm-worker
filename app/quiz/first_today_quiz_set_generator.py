import os
import logging
import random
import uuid
import pytz
from datetime import datetime

from core.database.database_manager import DatabaseManager
from constant.constant import FIRST_GENERATION_QUIZ_NUM


logging.basicConfig(level=logging.INFO)


def first_today_quiz_set_generator(
    member_id: int, 
    db_pk: int,
    ):
    db_manager = DatabaseManager(host=os.environ["PICKTOSS_DB_HOST"], user=os.environ["PICKTOSS_DB_USER"], password=os.environ["PICKTOSS_DB_PASSWORD"], db=os.environ["PICKTOSS_DB_NAME"])

    get_member_query = f"SELECT * FROM member WHERE id = {member_id}"
    member: dict = db_manager.execute_query(get_member_query)[0]
    
    # 처음으로 ai pick을 사용한 경우 바로 오늘의 퀴즈 생성
    if member['ai_pick_count'] == 1:
        get_quizzes_query = f"SELECT * FROM quiz WHERE document_id = {db_pk}"
        quizzes: list[dict] = db_manager.execute_query(get_quizzes_query)
        
        random.shuffle(quizzes)
        
        delivery_count = 0
        delivery_quizzes = []
        
        for quiz in quizzes:
            if delivery_count == FIRST_GENERATION_QUIZ_NUM:
                break
            delivery_count += 1
            delivery_quizzes.append(quiz)

        quiz_set_id = uuid.uuid4().hex
        quiz_set_insert_query = "INSERT INTO quiz_set (id, solved, is_today_quiz_set, member_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"
        timestamp_now = datetime.now(pytz.timezone('Asia/Seoul'))
        db_manager.execute_query(quiz_set_insert_query, (quiz_set_id, False, True, member['id'], timestamp_now, timestamp_now))        
        
        for delivery_quiz in delivery_quizzes:
            quiz_set_quiz_inset_query = "INSERT INTO quiz_set_quiz (quiz_id, quiz_set_id, created_at, updated_at) VALUES (%s, %s, %s, %s)"
            db_manager.execute_query(quiz_set_quiz_inset_query, (delivery_quiz['id'], quiz_set_id, timestamp_now, timestamp_now))
            
            quiz_delivered_count_update_query = f"UPDATE quiz SET delivered_count = delivered_count + 1 WHERE id = {delivery_quiz['id']}"
            db_manager.execute_query(quiz_delivered_count_update_query)
        
        db_manager.commit()
        
        db_manager.close()