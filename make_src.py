import os
import json
import time
import itertools

import cv2
import numpy as np

from dbConnect import insert_src_info, insert_src_ss_info

# 랜드마크 몰입도 모델 사용을 위해 포인트 간 거리 계산
# 68개 포인트 사용시, length = 2278
def cal_distance(points):
    point_combinations = itertools.combinations(points, 2)
    return np.array([((pair[1][0] - pair[0][0]) ** 2 + (pair[1][1] - pair[0][1]) ** 2) ** 0.5 for pair in point_combinations], dtype='float32')

# 감정 분류 로직
def predict_expression(immResult, landmark_68, width, height):
    try:        
        expResult = -1
        # 왼쪽 눈 최상단, 최하단 좌표 저장
        eye_upper, eye_lower = landmark_68[26], landmark_68[27]
        
        # 눈 감았는지 여부 확인
        is_eye_closed = eye_upper[1]*height - eye_lower[1]*height < 1.3

        # 눈감음 && 집중X => 졸음
        if is_eye_closed and immResult==1:
            expResult = 3
            return expResult

        # 윗입술 최하단, 아랫입술 최상단 좌표 저장
        ulip_middle, dlip_middle = landmark_68[14], landmark_68[3]
                
        # 입 벌림 여부 확인
        is_mouth_opend = (dlip_middle[1] - ulip_middle[1])*height > 1
        # 집중 && 입벌림 && 눈뜸 => 흥미로움
        if immResult==0 and is_mouth_opend and is_eye_closed == 0:
            expResult = 0
            return expResult
        # 집중X && 입벌림 && 눈뜸 => 지루함
        if immResult==1 and is_mouth_opend and is_eye_closed == 0:
            expResult = 2
            return expResult

        # 입다움 && 눈뜸 => 차분함
        if is_mouth_opend == 0 and is_eye_closed == 0:
            expResult = 1
            return expResult
        
        # 감정 분류 안될 경우 = -1
        return expResult
    except Exception as e:
        print(e)
        return expResult

# 몰입도&감정 결과 취합
def combination_result(immResult, expResult):
    if immResult==0 and expResult == 0:
        # 집중-흥미로움
        pred = '01'
    elif immResult==0 and expResult == 1:
        # 집중-차분함
        pred = '02'
    elif immResult==1 and expResult == 1:
        # 집중X-차분함
        pred = '03'
    elif immResult==1 and expResult == 2:
        # 집중X-지루함
        pred = '04'
    elif immResult==1 and expResult == 3:
        # 졸음
        pred = '05'
    else: 
        pred = '-1'
    
    return pred

# 통과율 계산 함수
def cal_ratio(user_info, input_path, output_path, output_dir, face_mesh, model, landmark_model):
    # 몰입도 모델의 인풋, 아웃풋 속성 로드
    model_input = model.get_input_details()[0]
    model_output = model.get_output_details()[0]

    # 원시 영상 로드
    capture = cv2.VideoCapture(input_path)

    # 양쪽 약 10%를 날리기 위해 원본 영상 너비(1080)에서 200 줄여서 저장
    w = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH)) - 200
    h = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = 30
    
    # 코덱 설정
    fourcc = cv2.VideoWriter_fourcc(*'H264')
    
    # 비디오 저장 핸들러 생성
    out = cv2.VideoWriter(output_path, fourcc, fps, (w, h))
    
    # 프레임 카운트
    frame_count = 0
    # 초 단위 시간 카운트
    sec = 0
    # 결과 저장 리스트 -> 통과율 계산용
    result_stats = []
    # 통과율 변수
    ratio = 0
    # 최종 결과 저장 리스트
    TOT = []
    # 이미지 저장 인덱스
    save_cnt = 0
    
    # 정답 label 지정
    label = input_path.split('-')[-1][:2]
    while capture.isOpened():
        # ret: 원본 비디오 리드 성공 여부
        # frame: 읽어들인 프레임 이미지
        ret, frame = capture.read()
        # 읽기 실패하면 반복문 종료
        if not ret:
            break
        
        # 양쪽 가로 100만큼 잘라서 저장
        # 통과율 반영되지 않은 pre 영상
        out.write(frame[:, 100:-100, :])

        # mediapipe 모델 사용하기 위해 RGB 채널로 변경
        frame.flags.writeable = False
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        result_fm = face_mesh.process(frame)

        # opencv 시각화를 위해 BGR 채널로 변경
        frame.flags.writeable = True
        frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)

        imm_pred = -1
        exp_pred = -1
        pred = -1
        status = -1
        
        # face mesh 모델 결과가 있을 때 실행
        if result_fm.multi_face_landmarks:
            # 인식된 결과 반복문 진행
            for face_landmarks in result_fm.multi_face_landmarks:
                # 모델 실행을 위한 랜드마크
                landmark_m = [(v.x, v.y) for i, v in enumerate(face_landmarks.landmark) if i in landmark_model]
                # 랜드마크 좌표들 사이의 거리 계산
                # 현재 distance length = 2278
                distance = cal_distance(landmark_m)
            
                # 몰입도 분류 0/1
                # 인풋 데이터 설정
                model.set_tensor(model_input["index"], distance.reshape(-1, 2278))
                # 모델 실행
                model.invoke()
                # 결과 저장 0:집중, 1:집중X
                imm_pred = 0 if model.get_tensor(model_output["index"])[0][0] < 0.5 else 1
                
                # 감정 분로 결과 저장
                exp_pred = predict_expression(imm_pred, landmark_m, frame.shape[1], frame.shape[0])
                
                # 결과 취합
                pred = combination_result(imm_pred, exp_pred)
                
                # 통과 여부 상태값
                # 실제 클래스와 예측 클래스가 동일하면 정답(1), 다르면 오답(0)
                # 추후 10초마다 통계내서 합불 결정
                status = 1 if label==pred else 0
                result_stats.append(status)

        # 현재 frame이 29 미만이면 1 증가, 29면 리셋
        # 로직 맨 마지막이기 때문에 지금 29면 다음 프레임 실행할 때 0으로 돌아가야됨
        frame_count = frame_count+1 if frame_count < 29 else 0
        
        # frame_count 이용해서 시간 측정
        # frame_count가 0이면 1초 증가
        # 30 프레임마다 0으로 회귀
        # 0,1,2,...,28,29,0,1,2,... 순서로 반복
        sec = sec + 1 if frame_count == 0 else sec

        # 10초 간격으로 결과 저장 리스트 flush
        if len(result_stats) == 300:
            st, et = sec-10, sec
            ratio = sum(result_stats) / 300
            TOT.append({
                'cls_cd': label,
                'pred': pred,
                'start_time': st,
                'end_time': et,
                'pass_ratio': ratio,
                'pass_status': 0 if ratio < 0.70 else 1
            })
            result_stats.clear()
        
        # esc 눌러서 종료
        if cv2.waitKey(33) & 0xFF == 27:
            cv2.destroyAllWindows()
            capture.release()
            break
    
    capture.release()
    # JSON 파일로 통과율 저장
    with open(output_path.replace('mp4', 'json'), 'w') as json_data:
        json.dump(TOT, json_data, indent=4)


# 통과율 기준으로 동영상 취합
def video_slice(video_path, json_path, output_dir):
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    start = time.time()
    
    # 원시 영상 경로 설정
    fileNM = os.path.split(video_path)[-1]
    output_path = os.path.join(output_dir, fileNM)
    print(output_path)
    
    # 통과율 json 로드
    jsonFile = open(json_path, 'r')
    jsonData = json.load(jsonFile)

    # pre 영상 로드
    capture = cv2.VideoCapture(video_path)
    width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = 30

    # 코덱 설정
    fourcc = cv2.VideoWriter_fourcc(*'H264')

    # 비디오 저장 핸들러 생성
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
    
    # 통과율이 0.7 이상인 구간만 저장
    for j in jsonData:
        if j['pass_ratio'] >= 0.7:
            # json에 있는 타임스탬프와 fps를 이용하여 시작 프레임과 종료 프레임 설정
            start_frame = int(j['start_time'] * fps)
            end_frame = int(j['end_time'] * fps)

            # 시작 프레임으로 이동
            capture.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

            # 시작 프레임 ~ 종료 프레임 구간 저장
            for _ in range(start_frame, end_frame):
                ret, frame = capture.read()
                if not ret:
                    break
                out.write(frame)
    
    capture.release()
    out.release()
    insert_src_info(output_path)
    print('소요시간:', time.time() - start)


# 이미지 캡처 함수
def video_snapshot(input_video, output_dir, user_info):
    # 원천 영상 로드
    capture = cv2.VideoCapture(input_video)
    
    # 프레임 수 카운트하면서 30번째마다 1초 증가
    frame_count = 0
    # 초 단위 시간 카운트
    sec = 0
    # 이미지 저장 시퀀스
    save_cnt = 0
    start = time.time()
    # 저장할 이미지 이름
    save_files = []
    while capture.isOpened():
        ret, frame = capture.read()
        if not ret:
            print('end of video')
            print('소요시간:', time.time() - start)
            break
        # 30프레임, 10초마다 이미지 저장
        if frame_count == 29 and sec % 10 == 0:
            # 시퀀스로 이미지 이름 증가
            save_path = os.path.join(output_dir, f'{user_info}_{save_cnt}.jpg')
            f = cv2.imwrite(save_path, frame)
            if f:
                insert_src_ss_info(save_path)
            save_cnt += 1
        
        # 현재 frame이 29 미만이면 1 증가, 29면 리셋
        # 로직 맨 마지막이기 때문에 지금 29면 다음 프레임 실행할 때 0으로 돌아가야됨
        frame_count = frame_count+1 if frame_count < 29 else 0
        
        # frame_count 이용해서 시간 측정
        # frame_count가 0이면 1초 증가
        # 30 프레임마다 0으로 회귀
        # 0,1,2,...,28,29,0,1,2,... 순서로 반복
        sec = sec + 1 if frame_count == 0 else sec
    
    capture.release()
    return save_files
