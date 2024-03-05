import os
import json

import cv2
import numpy as np

from dbConnect import insert_label_info, insert_label_ss_info

def make_label(user_info, input_path, output_dir, output_path, face_mesh, pose_detection, landmark_draw):
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    label = output_dir.split('/')[-1]

    # 원천 영상 로드
    capture = cv2.VideoCapture(input_path)
    w = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = 30

    # 코덱 설정
    fourcc = cv2.VideoWriter_fourcc(*'H264')

    # 동영상 저장 핸들러 생성
    out = cv2.VideoWriter(output_path, fourcc, fps, (w, h))
    
    sec = 0
    frame_count = 0
    save_cnt = 0
    save_img_files = []
    save_json_files = []
    while capture.isOpened():
        ret, frame = capture.read()
        if not ret:
            print('end of video')
            break
        
        # mediapipe 모델 사용하기 위해 RGB 채널로 변경
        frame.flags.writeable = False
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        result_fm = face_mesh.process(frame)
        result_ps = pose_detection.process(frame)

        # opencv 시각화를 위해 BGR 채널로 변경
        frame.flags.writeable = True
        frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)

        # 안면 바운딩박스, 안면 특징점
        if result_fm.multi_face_landmarks:
            # 478개 포인트 중 드로우에 사용할 68개 포인트만 추출
            face_landmark = np.array([(int(v.x*frame.shape[1]), int(v.y*frame.shape[0])) for i, v in enumerate(result_fm.multi_face_landmarks[0].landmark) if i in landmark_draw])            
            
            # 안면 특징점 드로우
            for p in face_landmark:
                cv2.circle(frame, (p[0], p[1]), 3, (0, 255, 0), -1)

            # 안면 특징점의 x좌표 최대,최소 / y좌표 최대,최소를 이용하여 안면 바운딩박스 설정
            x_min = int(face_landmark[:, 0].min())
            x_max = int(face_landmark[:, 0].max())
            y_min = int(face_landmark[:, 1].min())
            y_max = int(face_landmark[:, 1].max())

            # 안면 바운딩박스 드로우
            cv2.rectangle(frame, (x_min, y_min), (x_max, y_max), (0, 255, 0), 3)
            
        # 스켈레톤 드로우
        if result_ps.pose_landmarks:
            # 스켈레톤 드로우에 사용할 포인트만 추출
            pose_landmark = np.array([(int(v.x*frame.shape[1]), int(v.y*frame.shape[0])) for i, v in enumerate(result_ps.pose_landmarks.landmark) if i in range(11, 17)])
            
            # 스켈레톤 드로우
            for p in pose_landmark:
                cv2.circle(frame, (p[0], p[1]), 3, (0, 255, 0), -1)            

        # 30프레임, 10초마다 이미지 저장
        if frame_count == 29 and sec % 10 == 0:
            save_path = os.path.join(output_dir, f'{user_info}-{label}_{save_cnt}.jpg')
            save_img_files.append(save_path)
            f = cv2.imwrite(save_path, frame)
            
            # 좌표 정보 저장
            output_json = os.path.join(output_dir, f'{user_info}-{label}_{save_cnt}.json')
            save_json_files.append(output_json)
            result_json = {
                'cls_cd': label,
                'timestamp': sec,
                'bounding_box': [[x_min, y_min], [x_max, y_max]],
                'face_landmark': face_landmark.tolist(),
                'pose_landmark': pose_landmark.tolist()
            }

            # JSON 파일로 저장
            with open(output_json, 'w') as json_data:
                json.dump(result_json, json_data, indent=4)
            
            if f:
                insert_label_ss_info(save_path, output_json)
            save_cnt += 1
    
        # 현재 frame이 29 미만이면 1 증가, 29면 리셋
        # 로직 맨 마지막이기 때문에 지금 29면 다음 프레임 실행할 때 0으로 돌아가야됨
        frame_count = frame_count+1 if frame_count < 29 else 0
        
        # frame_count 이용해서 시간 측정
        # frame_count가 0이면 1초 증가
        # 30 프레임마다 0으로 회귀
        # 0,1,2,...,28,29,0,1,2,... 순서로 반복
        sec = sec + 1 if frame_count == 0 else sec
        out.write(frame)
    
    capture.release()
    out.release()
    insert_label_info(output_path)