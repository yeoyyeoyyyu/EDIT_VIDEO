import os
import json
import time
import traceback

import mediapipe as mp
import tensorflow as tf

from dbConnect import selectDB, update_step, insert_raw_info, insert_OX_info

from make_raw import make_raw
from make_src import cal_ratio, video_slice, video_snapshot
from make_label import make_label


# 데이터 가공 프로세스
class process:
    def __init__(self, user_id):
        # mediapipe 그리기 도구 로드(안면 바운딩박스, 안면 특징점, 스켈레톤 그리기 도구)
        mp_drawing = mp.solutions.drawing_utils
        mp_drawing_styles = mp.solutions.drawing_styles

        # mediapipe 모델 로드
        # face_mesh: 안면 특징점
        # pose: 스켈레톤
        mp_face_mesh = mp.solutions.face_mesh
        mp_pose_detection = mp.solutions.pose

        # mediapipe 모델 속성값 설정
        self.face_mesh = mp_face_mesh.FaceMesh(max_num_faces=1, refine_landmarks=True, min_detection_confidence=0.5, min_tracking_confidence=0.5)
        self.pose_detection = mp_pose_detection.Pose(min_detection_confidence=0.5)

        # AiDO-LOCA 몰입도 분류 모델 로드
        model_path = '몰입도 인식 모델 경로'
        self.model = tf.lite.Interpreter(model_path)

        # 모델에 메모리 할당
        self.model.allocate_tensors()

        # 478개의 포인트 중 추출해야할 68개의 인덱스
        # 그림 그릴 때 사용하는 랜드마크의 인덱스와 몰입도 분류 모델이 사용하는 랜드마크의 인덱스 정보가 다릅니다.
        with open(f'{os.path.dirname(os.path.abspath(os.path.dirname(__file__)))}/config/landmark.json', 'r') as json_file:
            landmark_json = json.load(json_file)
        # 그리기용 랜드마크 인덱스
        self.landmark_draw = landmark_json['draw']
        # 모델 추론용 랜드마크 인덱스
        self.landmark_model = landmark_json['model']
        # 랜드마크 각 포인트에 해당하는 이름 매핑
        self.ladmark_name = landmark_json['name']
        
        # 프로세스 시작
        self.process(user_id)

    def process(self, user_id):
        errNo = 0
        # 작업을 수행할 파일 로드
        try:
            # 파일 로드
            # 현재는 유저 아이디 기준 로드
            sql = f"select USER_ID, FILE_PATH, FILE_NM, TIME_LINE_JSON_DATA from tb_vdo_ori where USER_ID='{user_id}' and DEL_YN='N' and UPLOAD_YN='Y' order by VDO_CLS_SEQ;"
            
            # (USER_ID, FILE_PATH, FILE_NM, TIME_LINE_JSON_DATA) 형태의 튜플
            result = selectDB(sql)
            errNo += 1 # 0

            # result 튜플 예시
            # USER_ID            : aidyd8928
            # FILE_PATH          : /repo/data/02/78
            # FILE_NM            : 02-78-04--1-18-aidyd8928-00-001.webm
            # TIME_LINE_JSON_DATA: [{"cls_cd":"02","start_time":0,"end_time":160,"real_start_time":0,"real_end_time":160,"start_reg_dt":"20230906185314","end_reg_dt":"20230906185554"},]
            input_dir = result[0][1]
            branch = os.path.splitext(result[0][2])[0].split('-')
            user_info = '-'.join(branch[:-2])
            print('\nuser_info:', user_info)

            # 결과 저장 디렉토리 생성
            # 원시    : /repo/eduai/raw/02/82
            # 원천_pre: /repo/eduai/src/pre/02/82
            # 원천    : /repo/eduai/src/02/82
            # 가공    : /repo/eduai/label/02/82
            output_raw     = result[0][1].replace('data', 'eduai/raw')
            output_src_pre = result[0][1].replace('data', 'eduai/src/pre') + f'/{user_info}'
            output_src     = result[0][1].replace('data', 'eduai/src') + f'/{user_info}'
            output_label   = result[0][1].replace('data', 'eduai/label') + f'/{user_info}'

            print('output_raw:    ',     output_raw)
            print('output_src_pre:', output_src_pre)
            print('output_src:    ',     output_src)
            print('output_label:  ',   output_label, '\n')

            # 해당 경로에 디렉토리가 존재하지 않을 경우에만 생성
            if not os.path.isdir(output_raw):
                os.makedirs(output_raw)

            if not os.path.isdir(output_src_pre):
                os.makedirs(output_src_pre)

            if not os.path.isdir(output_src):
                os.makedirs(output_src)

            if not os.path.isdir(output_label):
                os.makedirs(output_label)
            errNo += 1 # 1

            print('-' * 20)
            print('start process...')
            # 원본 파일의 파일명과 타임라인 json을 각각 리스트에 저장
            webm_files = [values[2] for values in result]
            json_files = [values[3] for values in result]

            ##########################################################################################
            # 원시 데이터 가공: 롤링 파일 취합
            ##########################################################################################
            start = time.time()
            # webm_files에는 파일명만 있기 때문에 input_dir도 같이 전달 필요
            update_step(user_id, 1)
            raw_datas = make_raw(webm_files, json_files, input_dir, output_raw)
            print('make_raw 소요시간:', time.time()-start)
            update_step(user_id, 2)
            errNo += 1 # 2
            
            ##########################################################################################
            # 문제가 생긴 파일(배속 걸린 파일)에 대힌 로직 추가 개발 필요
            # 1. 원시 데이터 영상의 길이가 20분 미만인 경우에 webm2mp4 적용
            # 2. make_raw부터 다시 진행
            ##########################################################################################

            ##########################################################################################
            # 원시 데이터 정보 DB에 저장
            ##########################################################################################
            for raw in raw_datas:
                insert_raw_info(raw)

            ##########################################################################################
            # 원천_pre 데이터 생성(타임라인): 원천 데이터 생성을 위한 통과율 계산
            ##########################################################################################
            update_step(user_id, 3)
            start = time.time()
            for raw in raw_datas:
                fileNM = os.path.split(raw)[-1]
                pre_path = os.path.join(output_src_pre, fileNM)
                cal_ratio(user_info, raw, pre_path, output_src_pre, self.face_mesh, self.model, self.landmark_model)
            print('cal_ratio 소요시간:', time.time()-start)
            errNo += 1 # 3

            ##########################################################################################
            # 원천 데이터 생성: 통과율 충족하는 구간만 취합
            ##########################################################################################
            src_videos = []
            start = time.time()
            for raw in raw_datas:
                fileNM = os.path.split(raw)[-1]
                label = fileNM.split('-')[-1][:2]
                pre_path = os.path.join(output_src_pre, fileNM)
                output_src_label = output_src + f'/{label}'
                src_path = video_slice(pre_path, pre_path.replace('mp4', 'json'), output_src_label)
                src_videos.append(src_path)
            print('video_slice 소요시간:', time.time()-start)
            errNo += 1 # 4

            ##########################################################################################
            # 원천 데이터 이미지 스냅샷
            ##########################################################################################
            src_images = []
            start = time.time()
            for raw in raw_datas:
                fileNM = os.path.split(raw)[-1]
                label = fileNM.split('-')[-1][:2]
                output_src_label = output_src + f'/{label}'
                src_path = os.path.join(output_src, fileNM)
                src_ss = video_snapshot(src_path, output_src_label, user_info)
                src_images.append(src_ss)
            print('video_snapshot 소요시간:', time.time()-start)

            update_step(user_id, 4)
            errNo += 1 # 5        
            
            ##########################################################################################
            # 가공 데이터 생성: 원천 데이터에 안면 바운딩박스, 안면 특징점, 스켈레톤 포인트 드로우
            ##########################################################################################
            start = time.time()
            update_step(user_id, 5)

            for raw in raw_datas:
                fileNM = os.path.split(raw)[-1]
                label = fileNM.split('-')[-1][:2]
                input_dir = output_src + f'/{label}'
                input_path = os.path.join(input_dir, fileNM)
                output_label_label = output_label + f'/{label}'

                make_label(
                            user_info,
                            input_path,
                            output_label_label,
                            os.path.join(output_label_label, fileNM),
                            self.face_mesh,
                            self.pose_detection,
                            self.landmark_draw
                )
            print('make_label 소요시간:', time.time()-start)

            update_step(user_id, 6)
            errNo += 1 # 6

        except Exception as e:
            print(errNo)
            print(traceback.format_exc())