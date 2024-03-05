import json
import multiprocessing as mp

from main import process


if __name__=="__main__":
    # config 파일에서 인식해야할 유저ID 로드
    # user_info 테이블 읽어서 분리하는 방법으로 변경 필요
    json_path = '/root/eduai/config/config.json'
    jsonFile = open(json_path, 'r')
    jsonData = json.load(jsonFile)
    user_info = jsonData['user_info']

    # 최대 cpu 사용 수 지정
    max_process = mp.cpu_count() // 2
    print(f'max_process: {max_process}, user_info count: {len(user_info)}')
    
    # user_info 리스트의 길이가 max_process 보다 큰 경우 max_process만큼만 진행하도록 조정
    # 나머지 인원은 다음 배치 때 진행
    max_process = max_process if user_info > max_process else user_info
    procs = []
    try:
        manager = mp.Manager()
        for i in range(max_process):
            # main.process에 유저ID 전달하며 호출
            proc = mp.Process(target=process, args=(user_info[i],))
            
            # 프로세스 시작
            procs.append(proc)
            proc.start()

        # 모든 프로세스 종료될 때까지 대기
        for proc in procs:
            proc.join()
    except Exception as e:
        print(e)