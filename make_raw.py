import os
import time
import cv2
import json

def make_raw(webm_files, json_files, input_dir, output_dir):
    try:
        # 비디오 정보 가져오기
        capture = cv2.VideoCapture(os.path.join(input_dir, webm_files[0]))
        w = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
        h = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = 30

        # 코덱 설정
        fourcc = cv2.VideoWriter_fourcc(*'H264')

        # 비디오 핸들러 딕셔너리
        out_handles = {}
        out_counts = {'01':0, '02':0, '03':0, '04':0, '05':0}

        # 클래스별 비디오 핸들러 저장
        file = webm_files[0].split('-')
        output_files = []
        for i in ['01', '02', '03', '04', '05']:
            file[-2] = i + '.mp4'
            # 클래스별 동영상 저장 경로 설정
            output_path = os.path.join(output_dir, '-'.join(file[:-1]))
            output_files.append(output_path)
            # 클래스별 핸들러 딕셔너리에 핸들러 객체 할당
            out_handles[i] = cv2.VideoWriter(output_path, fourcc, fps, (w, h))
        
        # 비디오 정보 저장용 핸들러 릴리즈
        capture.release()
        
        # 구간별 소요시간 측정을 위한 초기값 세팅
        start = time.time()
        # 롤링 비디오 파일 리스트, 타임라인 json 파일 리스트 반복문
        for i, (w, j) in enumerate(zip(webm_files, json_files)):
            print(f'make raw {i}/{len(webm_files)} start...')
            # 문자열로 된 timeline json을 json 객체로 구조화
            jsonData = json.loads(j)
            
            # 원본 webm 파일 로드
            capture = cv2.VideoCapture(os.path.join(input_dir, w))

            # json의 인덱스
            # 분류 클래스 롤링하면서 결과 들어옴
            idx = 0

            # 프레임 수 카운트하면서 30번째마다 1초 증가
            frame_count = 0

            # 초 단위 시간 카운트
            sec = 0

            # json 파일의 종료 시간 (클래스 바뀌는 시점)
            timecheck = jsonData[idx]['real_end_time']

            # json 파일에 있는 정답 클래스
            label = jsonData[idx]['cls_cd']
            print('idx:', idx)
            print('label:', label)

            # 초기 세팅 종료 후 원본 비디오 리드 반복문
            while capture.isOpened():
                # ret: 원본 비디오 리드 성공 여부
                # frame: 읽어들인 프레임 이미지
                ret, frame = capture.read()
                # 읽기 실패하면 반복문 종료
                if not ret:
                    break
                
                # 현재 시간이 클래스 전환 기준 시간보다 크다면
                # 인덱스 증가, 기준 시간 변경, 정답 클래스 변경
                if sec > timecheck:
                    idx += 1
                    if idx > 4:
                        break
                    print('idx:', idx)
                    timecheck = jsonData[idx]['real_end_time']
                    label = jsonData[idx]['cls_cd']
                    print('label:', label)
                # 읽은 프레임을 타임라인에 있는 정답 클래스에 저장
                out_handles[label].write(frame)
                
                # frame을 30개 진행할 때마다 1초 증가
                frame_count = frame_count+1 if frame_count < 29 else 0            
                sec = sec + 1 if frame_count == 0 else sec
            capture.release()
            print('소요시간:', time.time() - start)
        print('저장 종료')
    except Exception as e:
        print('make_raw error')
        print('error:', e)
    finally:
        # 비디오 저장 핸들러 릴리즈
        for key in out_handles.keys():
            out_handles[key].release()
    return output_files
# make_raw('02-78-04--1-18-qkrxorb04-00-001.webm', '02-78-04--1-18-qkrxorb04-00-001.json', '/repo/data/02/78')