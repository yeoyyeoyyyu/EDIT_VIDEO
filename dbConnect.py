import os
import json

import cv2
import pymysql

db_info_path = f'{os.path.dirname(os.path.abspath(os.path.dirname(__file__)))}/config/config.json'
print(db_info_path)

# db 
with open(db_info_path, 'r') as jsonFile:
    jsonData = json.load(jsonFile)
db_info = jsonData['db_info']
# print(db_info)

def insertDB(sql):
    '''
    데이터 insert 함수
    parameter:
        insert query
    '''
    try:
        conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close()
    except Exception as e:
        print(e)

def selectDB(sql):
    '''
    데이터 select 함수
    parameter:
        select query
    '''
    try:
        conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        return result
    except Exception as e:
        print(e)

def updateDB(sql):
    '''
    데이터 update 함수
    parameter:
        update query
    '''
    try:
        conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close()
    except Exception as e:
        print(e)


def get_filesize(file_path):
    '''
    file의 크기를 계산하는 함수
    parameter:
        file_path: 파일의 절대 경로
    return:
        file 용량(Byte)
    '''
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
    else:
        file_size = 0
    return file_size


def get_rec_time(file_path):
    '''
    동영상의 길이(시간)을 확인하는 함수
    parameter:
        file_path: 파일의 절대 경로
    return:
        동영상의 길이(초)
    '''
    capture = cv2.VideoCapture(file_path)
    frames = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = int(capture.get(cv2.CAP_PROP_FPS))
    return frames / fps
    

def update_step(user_id, step):
    '''
    tb_vdo_step 업데이트 함수
    parameter:
        user_id: 유저ID
        step: 현재 완료된 프로세스 단계
            0: 진행 이전
            1: 비디오_원본_클래스/비디오판단 적재중 
            2: 비디오_원본_클래스/비디오판단 완료 
            3: TB_비디오/TB_비디오_스샷_이미지 적재중 
            4: TB_비디오/TB_비디오_스샷_이미지 적재완료
            5: TB_비디오_라벨링/TB_비디오_라벨링_스샷_이미지 적재중 
            6: TB_비디오_라벨링/TB_비디오_라벨링_스샷_이미지 적재완료
    '''
    try:
        conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
        cur = conn.cursor()
        sql = f"UPDATE TB_VDO_STEP SET PROCESS_STS = {step}, UPD_DTTM = NOW(), UPD_ID = 'batch' WHERE USER_ID = '{user_id}'"
        cur.execute(sql)
        conn.commit()
        conn.close()
    except:
        print(sql)



def insert_raw_info(file_path):
    '''
    tb_vdo_ori_cls 테이블에 데이터 insert 함수
    parameter:
        file_path: 원시 데이터 절대 경로
    '''
    USER_ID = file_path.split('-')[-2]
    VDO_CLS_CD = file_path.split('-')[-1].split('.')[0]
    FILE_PATH, FILE_NM = os.path.split(file_path)
    FILE_SIZE = get_filesize(file_path)
    FILE_EXT = os.path.splitext(FILE_NM)[-1][1:]
    REC_TIME_SEC = get_rec_time(file_path)
    ONF_STS_CD = '4'
    DEL_YN = 'N'
    REG_ID = USER_ID
    UPD_ID = 'batch'

    sql = "INSERT INTO tb_vdo_ori_cls" + " " + \
          "(" + \
            "VDO_ORI_CLS_SNO" + "," + \
            "USER_ID" + "," + \
            "VDO_CLS_CD" + "," + \
            "FILE_PATH" + "," + \
            "FILE_NM" + "," + \
            "FILE_SIZE" + "," + \
            "FILE_EXT" + "," + \
            "REC_TIME_SEC" + "," + \
            "DEL_YN" + "," + \
            "REG_ID" + "," + \
            "REG_DTTM" + "," + \
            "UPD_ID" + "," + \
            "UPD_DTTM" + \
          ")" + " " + \
          "VALUES" + " " + \
          "(" + \
            "F_GET_SEQ('VDO_ORI_CLS_SNO')" + "," + \
            "'" + USER_ID + "'" + "," + \
            "'" + VDO_CLS_CD + "'" + "," + \
            "'" + FILE_PATH + "'" + "," + \
            "'" + FILE_NM + "'" + "," + \
            str(FILE_SIZE) + "," + \
            "'" + FILE_EXT + "'" + "," + \
            "'" + str(REC_TIME_SEC) + "'" + "," + \
            "'" + DEL_YN + "'" + "," + \
            "'" + REG_ID + "'" + "," + \
            "NOW()" + "," + \
            "'" + UPD_ID + "'" + "," + \
            "NOW()" + \
           ")"

    conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()


def select_raw_id(user_id, cls_cd):
    '''
    유저ID를 이용하여 원시 데이터의 비디오ID 조회
    parameter:
        user_id: 유저ID
        cls_cd: 클래스 넘버
    '''
    try:
        sql = f"SELECT VDO_ORI_CLS_SNO FROM tb_vdo_ori_cls WHERE USER_ID = '{user_id}' AND VDO_CLS_CD = '{cls_cd}';"
        conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchone()[0]
        return result
    except Exception as e:
        print(e)

def insert_OX_info(file_path):
    '''
    ********* 원시 영상 데이터 경로 입력 받아서 아래 parameter들 생성하도록 수정
    tb_vdo_jgmt 테이블에 오토라벨링 판단 결과 저장 함수
    parameter:
        VOD_ORI_CLS_SNO: 비디오 원본 클래스 일련번호
        USER_ID        : 유저ID
        STAR_PLY_SEC   : 플레이 시작 시간(초)
        STAR_END_SEC   : 플레이 종료 시간(초)
        VDO_AI_CLS_CD  : 비디오 AI 판단 분류 코드
        REG_ID         : 입력ID
    추가 개발 요소:
        ADO_AI_CLS_CD  : 오디오 AI판단 분류 코드
    '''
    try:
        raw_fileNM = os.path.split(file_path)[-1]
        branch = raw_fileNM.split('-')
        user_info = '-'.join(branch[:-1])

        USER_ID = file_path.split('-')[-2]
        VDO_CLS_CD = file_path.split('-')[-1].split('.')[0]
        VDO_ORI_CLS_SNO = select_raw_id(USER_ID, VDO_CLS_CD)

        VDO_JGMT_SNO = "F_GET_SEQ('VDO_JGMT_SNO')"
        REG_ID = USER_ID
        REG_DTTM     = 'NOW()'
        UPD_ID       = 'batch'
        UPD_DTTM     = 'NOW()'

        sql = "INSERT INTO tb_vdo_jgmt" + " " + \
                "(" + \
                    "VDO_ORI_CLS_SNO" + "," + \
                    "USER_ID" + "," + \
                    "STAR_PLY_SEC" + "," + \
                    "STAR_END_SEC" + "," + \
                    "VDO_AI_CLS_CD" + "," + \
                    "REG_ID" + "," + \
                    "REG_DTTM" + "," + \
                    "UPD_ID" + "," + \
                    "UPD_DTTM" + \
                ")" + " " + \
                "VALUES" + " " + \
                "(" + \
                    f"'{VDO_ORI_CLS_SNO}'" + "," + \
                    f"'{USER_ID}'" + "," + \
                    f"'{STAR_PLY_SEC}'" + "," + \
                    f"'{STAR_END_SEC}'" + "," + \
                    f"'{VDO_AI_CLS_CD}'" + "," + \
                    f"'{REG_ID}'" + "," + \
                    f"{REG_DTTM}" + "," + \
                    f"'{UPD_ID}'" + "," + \
                    f"{UPD_DTTM}" + \
                ");"
        # print(sql)
        conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close()
    except:
        print(sql)


def insert_src_info(file_path):
    '''
    tb_vdo 테이블에 원천 영상 데이터 정보 저장
    parameter:
        file_path: 원천 영상 데이터 절대 경로
    '''
    try:
        raw_fileNM = os.path.split(file_path)[-1]
        branch = raw_fileNM.split('-')
        user_info = '-'.join(branch[:-1])

        USER_ID = file_path.split('-')[-2]
        VDO_CLS_CD = file_path.split('-')[-1].split('.')[0]
        VDO_ORI_CLS_SNO = select_raw_id(USER_ID, VDO_CLS_CD)

        FILE_PATH, FILE_NM = os.path.split(file_path)
        FILE_SIZE = get_filesize(file_path)
        FILE_EXT = os.path.splitext(file_path)[-1].split('.')[-1]
        REG_ID = USER_ID
        REG_DTTM = 'NOW()'
        UPD_ID = 'batch'
        UPD_DTTM = 'NOW()'
        
        sql = "INSERT INTO tb_vdo" + " " + \
                "(" + \
                    "VDO_ORI_CLS_SNO" + "," + \
                    "USER_ID" + "," + \
                    "FILE_PATH" + "," + \
                    "FILE_NM" + "," + \
                    "FILE_SIZE" + "," + \
                    "FILE_EXT" + "," + \
                    "REG_ID" + "," + \
                    "REG_DTTM" + "," + \
                    "UPD_ID" + "," + \
                    "UPD_DTTM" + \
                ")" + " " + \
                "VALUES" + " " + \
                "(" + \
                    f"'{VDO_ORI_CLS_SNO}'" + "," + \
                    f"'{USER_ID}'" + "," + \
                    f"'{FILE_PATH}'" + "," + \
                    f"'{FILE_NM}'" + "," + \
                    f"{FILE_SIZE}" + "," + \
                    f"'{FILE_EXT}'" + "," + \
                    f"'{REG_ID}'" + "," + \
                    f"{REG_DTTM}" + "," + \
                    f"'{UPD_ID}'" + "," + \
                    f"{UPD_DTTM}" + \
                ");"

        conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close()
    except:
        print(sql)

def insert_src_ss_info(file_path):
    '''
    tb_vdo_ss_img 테이블에 원천 이미지 데이터 정보 저장
    parameter:
        file_path: 원천 이미지 데이터 절대 경로
    '''
    try:
        FILE_PATH, FILE_NM = os.path.split(file_path)
        USER_ID = FILE_NM.split('_')[0]
        VDO_CLS_CD = file_path.split('/')[-2]
        VDO_ORI_CLS_SNO = select_raw_id(USER_ID, VDO_CLS_CD)

        SS_IMG_SEQ = os.path.splitext(FILE_NM)[0].split('_')[-1]
        SS_TIME_SEC = int(SS_IMG_SEQ) * 10
        FILE_SIZE = get_filesize(file_path)
        FILE_EXT = os.path.splitext(FILE_NM)[-1][1:]
        REG_ID = USER_ID
        REG_DTTM = 'NOW()'
        UPD_ID = 'batch'
        UPD_DTTM = 'NOW()'

        sql = "INSERT INTO tb_vdo_ss_img" + " " + \
                "(" + \
                    "VDO_ORI_CLS_SNO" + "," + \
                    "SS_IMG_SEQ" + "," + \
                    "USER_ID" + "," + \
                    "SS_TIME_SEC" + "," + \
                    "FILE_PATH" + "," + \
                    "FILE_NM" + "," + \
                    "FILE_SIZE" + "," + \
                    "FILE_EXT" + "," + \
                    "REG_ID" + "," + \
                    "REG_DTTM" + "," + \
                    "UPD_ID" + "," + \
                    "UPD_DTTM" + \
                ")" + " " + \
                "VALUES" + " " + \
                "(" + \
                    f"'{VDO_ORI_CLS_SNO}'" + "," + \
                    f"{SS_IMG_SEQ}" + "," + \
                    f"'{USER_ID}'" + "," + \
                    f"'{SS_TIME_SEC}'" + "," + \
                    f"'{FILE_PATH}'" + "," + \
                    f"'{FILE_NM}'" + "," + \
                    f"{FILE_SIZE}" + "," + \
                    f"'{FILE_EXT}'" + "," + \
                    f"'{REG_ID}'" + "," + \
                    f"{REG_DTTM}" + "," + \
                    f"'{UPD_ID}'" + "," + \
                    f"{UPD_DTTM}" + \
                ");"

        conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close()
    except:
        print(sql)



def insert_label_info(file_path):
    '''
    tb_vdo_lb 테이블에 가공 영상 데이터 정보 저장
    parameter:
        file_path: 가공 영상 데이터 절대 경로
    '''
    try:
        raw_fileNM = os.path.split(file_path)[-1]
        branch = raw_fileNM.split('-')
        user_info = '-'.join(branch[:-1])

        USER_ID = file_path.split('-')[-2]
        VDO_CLS_CD = file_path.split('-')[-1].split('.')[0]
        VDO_ORI_CLS_SNO = select_raw_id(USER_ID, VDO_CLS_CD)

        FILE_PATH, FILE_NM = os.path.split(file_path)
        FILE_SIZE = get_filesize(file_path)
        FILE_EXT = os.path.splitext(file_path)[-1].split('.')[-1]
        REG_ID = USER_ID
        REG_DTTM = 'NOW()'
        UPD_ID = 'batch'
        UPD_DTTM = 'NOW()'
        
        sql = "INSERT INTO tb_vdo_lb" + " " + \
                "(" + \
                    "VDO_ORI_CLS_SNO" + "," + \
                    "USER_ID" + "," + \
                    "FILE_PATH" + "," + \
                    "FILE_NM" + "," + \
                    "FILE_SIZE" + "," + \
                    "FILE_EXT" + "," + \
                    "REG_ID" + "," + \
                    "REG_DTTM" + "," + \
                    "UPD_ID" + "," + \
                    "UPD_DTTM" + \
                ")" + " " + \
                "VALUES" + " " + \
                "(" + \
                    f"'{VDO_ORI_CLS_SNO}'" + "," + \
                    f"'{USER_ID}'" + "," + \
                    f"'{FILE_PATH}'" + "," + \
                    f"'{FILE_NM}'" + "," + \
                    f"{FILE_SIZE}" + "," + \
                    f"'{FILE_EXT}'" + "," + \
                    f"'{REG_ID}'" + "," + \
                    f"{REG_DTTM}" + "," + \
                    f"'{UPD_ID}'" + "," + \
                    f"{UPD_DTTM}" + \
                ");"

        conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close()
    except:
        print(sql)

def insert_label_ss_info(image_path, json_path):
    '''
    tb_vdo_lb_ss_img 테이블에 가공 이미지 데이터 정보 저장
    parameter:
        image_path: 가공 이미지 데이터 절대 경로
        json_path: 가공 이미지 데이터의 json 절대 경로
    '''
    try:
        with open(json_path, 'r') as jsonFile:
            jsonData = json.load(jsonFile)

        FILE_PATH, FILE_NM = os.path.split(image_path)
        USER_ID = image_path.split('-')[-2]
        VDO_CLS_CD = image_path.split('/')[-2]
        VDO_ORI_CLS_SNO = select_raw_id(USER_ID, VDO_CLS_CD)

        SS_IMG_SEQ = os.path.splitext(FILE_NM)[0].split('_')[-1]
        SS_TIME_SEC = int(SS_IMG_SEQ) * 10
        FILE_SIZE = get_filesize(image_path)
        FILE_EXT = os.path.splitext(FILE_NM)[-1][1:]
        JSON_FILE_PATH, JSON_FILE_NM = os.path.split(json_path)
        JSON_FILE_SIZE = get_filesize(json_path)
        JSON_FILE_EXT = os.path.splitext(JSON_FILE_NM)[-1][1:]
        MK_UP_JSON_DATA = str(jsonData).replace("'", '"')
        REG_ID = USER_ID
        REG_DTTM = 'NOW()'
        UPD_ID = 'batch'
        UPD_DTTM = 'NOW()'

        sql = "INSERT INTO tb_vdo_lb_ss_img" + " " + \
                "(" + \
                    "VDO_ORI_CLS_SNO" + "," + \
                    "SS_IMG_SEQ" + "," + \
                    "USER_ID" + "," + \
                    "SS_TIME_SEC" + "," + \
                    "FILE_PATH" + "," + \
                    "FILE_NM" + "," + \
                    "FILE_SIZE" + "," + \
                    "FILE_EXT" + "," + \
                    "JSON_FILE_PATH" + "," + \
                    "JSON_FILE_NM" + "," + \
                    "JSON_FILE_SIZE" + "," + \
                    "JSON_FILE_EXT" + "," + \
                    "MK_UP_JSON_DATA" + "," + \
                    "REG_ID" + "," + \
                    "REG_DTTM" + "," + \
                    "UPD_ID" + "," + \
                    "UPD_DTTM" + \
                ")" + " " + \
                "VALUES" + " " + \
                "(" + \
                    f"'{VDO_ORI_CLS_SNO}'" + "," + \
                    f"{SS_IMG_SEQ}" + "," + \
                    f"'{USER_ID}'" + "," + \
                    f"'{SS_TIME_SEC}'" + "," + \
                    f"'{FILE_PATH}'" + "," + \
                    f"'{FILE_NM}'" + "," + \
                    f"{FILE_SIZE}" + "," + \
                    f"'{FILE_EXT}'" + "," + \
                    f"'{JSON_FILE_PATH}'" + "," + \
                    f"'{JSON_FILE_NM}'" + "," + \
                    f"'{JSON_FILE_SIZE}'" + "," + \
                    f"'{JSON_FILE_EXT}'" + "," + \
                    f"'{MK_UP_JSON_DATA}'" + "," + \
                    f"'{REG_ID}'" + "," + \
                    f"{REG_DTTM}" + "," + \
                    f"'{UPD_ID}'" + "," + \
                    f"{UPD_DTTM}" + \
                ");"

        conn = pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], db=db_info['db'], port=3306)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close()
    except Exception as e:
        print(e)
        print(sql)