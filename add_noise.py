'''
main()          : 오디오 합성 매니지드 함수
add_noise()     : 원본 오디오에 노이즈 추가 함수
add_silence()   : 원본 오디오에 무음 추가 함수
collect_audio() : 구간별 오디오 취합 함수

calc_adj_rms    : SNR을 기반으로 조정된 RMS를 계산하는 함수, add_noise 함수 내에서만 사용
calc_ampitude   : 오디오의 진폭을 계산하는 함수
calc_rms        : 오디오의 RMS를 계산하는 함수
'''
import os
import time
import wave
import array

import numpy as np


def calc_adj_rms(input_rms, snr):
    '''
    SNR(Signal-to-Noise Ratio, 신호 대 잡음비)을 기반으로 조정된 RMS를 계산하는 함수
    SNR이 낮을수록 노이즈가 크게 합성됨
    parameter:
        input_rms: 원본 오디오 신호의 RMS
        snr      : 신호:잡음 비율
    return:
        조정된 RMS
    '''
    return input_rms / (10 ** (snr / 20))


def calc_ampitude(wf):
    '''
    wave 파일에서 진폭을 계산하는 함수
    parameter:
        wf: wave 파일 객체
    return:
        진폭 배열
    '''
    buffer = wf.readframes(wf.getnframes())
    return np.frombuffer(buffer, dtype="int16").astype(np.float64)


def calc_rms(amp):
    '''
    오디오의 진폭에서 RMS를 계산하는 함수
    parameter:
        amp: 진폭 배열
    return:
        RMS
    '''
    return np.sqrt(np.mean(np.square(amp)))


def add_noise(input_amp, sample_rate, n_channels, timestamp, noise_path, snr=5):
    '''
    오디오에서 지정된 시간에 노이즈 추가
    parameter:
        input_amp   : 입력 오디오의 진폭 배열
        sample_rate : 입력 오디오의 샘플링 레이트
        n_channels  : 입력 오디오의 채널 수 (모노=1, 스테레오=2)
        timestamp  : 노이즈를 추가할 시간 구간 리스트 [(시작1, 끝1), (시작2, 끝2), ...]
        noise_path  : 노이즈 파일 경로 리스트
        snr         : 신호 대 잡음비 (dafault=5)
    return:
        노이즈가 추가된 오디오 배열
    '''
    # 노이즈가 추가된 오디오 저장할 배열에 input_amp 복사
    mixed_amp = input_amp.copy()
    
    # 입력 오디오의 RMS 계산
    input_rms = calc_rms(input_amp)

    # timestamp를 기준으로 노이즈 추가
    for idx, (start_sec, end_sec) in enumerate(timestamp):
        # timestamp의 시작과 끝 인덱스를 계산
        start_index = int(start_sec * sample_rate * n_channels)
        end_index = int(end_sec * sample_rate * n_channels)

        # 노이즈 파일의 진폭
        with wave.open(noise_path[idx], "r") as noise_wav:
            noise_amp = calc_ampitude(noise_wav)

        # SNR 값으로 노이즈의 진폭을 조정
        adjusted_noise_rms = calc_adj_rms(input_rms, snr)
        noise_amp *= (adjusted_noise_rms / calc_rms(noise_amp))

        # 노이즈의 길이를 timestamp의 길이에 맞춰 조정
        final_noise_block_length = end_index - start_index
        if len(noise_amp) > final_noise_block_length:
            # 노이즈 파일의 길이가 삽입 구간 보다 길면 노이즈 길이 단축
            noise_amp = noise_amp[:final_noise_block_length]
        else:
            # 노이즈 파일의 길이가 삽입 구간 보다 짧다면 노이즈 파일 반복하여 연장
            noise_amp = np.tile(noise_amp, final_noise_block_length // len(noise_amp) + 1)[:final_noise_block_length]

        # 조정된 노이즈를 오디오 신호에 합성
        mixed_amp[start_index:end_index] += noise_amp

    # 노이즈가 추가된 오디오 신호를 반환
    return mixed_amp



def add_silence(amp, sample_rate, n_channels, silence_timestamp):
    '''
    오디오에 무음 추가하는 함수
    parameter:
        amp         : 입력 오디오의 진폭 배열
        sample_rate : 샘플링 레이트
        n_channels  : 채널 수 (모노=1, 스테레오=2)
        timestamp   : 무음을 추가할 시간 구간 리스트 [(시작1, 끝1), (시작2, 끝2), ...]
    return:
        무음이 추가된 오디오 배열
    '''
    # 무음을 추가할 오디오 배열에 원본 오디오 복사
    silent_amp = amp.copy()
    
    # 무음을 추가할 구간 반복
    for start_sec, end_sec in silence_timestamp:        
        # 무음을 추가할 구간의 시작과 끝 인덱스를 계산
        start_index = int(start_sec * sample_rate * n_channels)
        end_index = int(end_sec * sample_rate * n_channels)
        
        # 해당 구간의 진폭을 0으로 설정하여 무음 추가
        silent_amp[start_index:end_index] = 0.0
    
    # 무음 구간 추가된 오디오 반환
    return silent_amp


def collect_audio(input_amp, sample_rate, n_channels, timestamp):
    '''
    오디오에서 원하는 구간만 취합하는 함수
    parameter:
        input_amp   : 입력 오디오의 진폭 배열
        sample_rate : 샘플링 레이트
        n_channels  : 채널 수 (모노=1, 스테레오=2)
        timestamp   : 선택할 시간 구간 리스트 [(시작1, 끝1), (시작2, 끝2), ...]
    return:
        취합된 구간의 오디오 배열
    '''
    # 취합할 오디오 구간을 저장할 배열
    save_amp = np.array([], dtype=np.float64)
    
    # 지정된 시간 간격에 따라 오디오 취합
    for start_sec, end_sec in timestamp:
        # 저장할 구간의 시작과 끝 인덱스를 계산
        start_index = int(start_sec * sample_rate * n_channels)
        end_index = int(end_sec * sample_rate * n_channels)

        # 저장할 구간의 오디오를 save_amp 추가
        save_amp = np.concatenate([save_amp, input_amp[start_index:end_index]])
    
    # 취합된 오디오 반환
    return save_amp


# 메인 함수
def main(input, output, noise_path, snr=5, timestamp=None, silence_timestamp=None, collect_timestamp=None):
    '''
    오디오 합성 매니저 함수
    parameter:
        input             : 인풋 오디오 파일 절대 경로
        output            : 아웃풋 오디오 파일 절대 경로
        noise_path        : 노이즈 오디오 파일 절대 경로

        snr               : 신호:잡음 계수

        timestamp         : 노이즈를 합성할 구간, start_time&end_time 필요
                            ex) [(0, 10), (10, 40), (40, 60), (60, 70)]
        silence_timestamp : 무음을 합성할 구간, start_time&end_time 필요
                            ex) [(0, 10), (10, 40), (40, 60), (60, 70)]
        collect_timestamp  : 오디오를 취합할 구간, start_time&end_time 필요
                            ex) [(0, 10), (10, 40), (40, 60), (60, 70)]
    '''
    errNo = 0
    errMsg = ['오디오 파일 로드 에러', '소음 추가 에러', '무음 추가 에러', '오디오 취합 에러', '오디오 정규화 에러', '오디오 저장 세팅 에러', '오디오 저장 에러']
    try:
        # 입력 파일을 열어 오디오 파라미터를 추출
        with wave.open(input, "r") as input_wav:
            sample_rate, n_channels = input_wav.getframerate(), input_wav.getnchannels()
            
            # 인풋 오디오의 진폭을 계산합니다.
            input_amp = calc_ampitude(input_wav)
        errNo += 1

        # 노이즈를 추가할 오디오 배열을 초기화
        mixed_amp = input_amp.copy()

        # 노이즈를 합성할 시간 구간이 있는 경우 노이즈 추가
        if timestamp and noise_path:
            mixed_amp = add_noise(input_amp, sample_rate, n_channels, timestamp, noise_path, snr)
        errNo += 1

        # 무음 구간이 있는 경우 무음 추가
        if silence_timestamp:
            mixed_amp = add_silence(mixed_amp, sample_rate, n_channels, silence_timestamp)
        errNo += 1

        # 오디오 취합 구간이 있는 경우 오디오 취합
        if collect_timestamp:
            mixed_amp = collect_audio(mixed_amp, sample_rate, n_channels, collect_timestamp)
        errNo += 1
        
        # 음성절단현상이 일어나지않게 정규화
        if mixed_amp.max() > 32767:
            mixed_amp *= (32767 / mixed_amp.max())
        errNo += 1
        
        # 오디오 저장 핸들러 생성
        output_wave = wave.open(output, "w")
        # 오디오 저장 파라미터 설정
        output_wave.setparams(input_wav.getparams())
        errNo += 1
        
        # 노이즈가 추가된 오디오 데이터 저장
        output_wave.writeframes(array.array('h', mixed_amp.astype(np.int16)).tobytes())
        # 오디오 저장 핸들러 종료
        output_wave.close()
        errNo += 1
    except Exception as e:
        print(errMsg[errNo])
        print(e)

if __name__ == '__main__':
    start = time.time()
    # audio 파일 디렉토리
    audio_dir = 'audio_file_path'

    # input audio 절대 경로
    input = os.path.join(audio_dir, 'sample.wav')
    # output audio 절대 경로
    output = os.path.join(audio_dir, 'sample_result.wav')
    # noise audio 절대 경로
    noise_path = [os.path.join(audio_dir, 'car_horn.wav'), 
                  os.path.join(audio_dir, 'keyboard.wav'), 
                  os.path.join(audio_dir, 'loud.wav'), 
                  os.path.join(audio_dir, 'siren.wav')]
    snr = 5
    # 경적+컨텐츠 - 무음 - 컨텐츠 - 키보드+컨텐츠
    # 군중+컨텐츠 - 컨텐츠 - 무음 - 컨텐츠 - 사이렌+컨텐츠
    timestamp = [(0, 5), (15, 20), (50, 55), (70, 75)]
    silence_timestamp = [(5, 10), (60, 65)]
    collect_timestamp = [(0, 20), (50, 75)]
    main(input, output, noise_path, snr, timestamp, silence_timestamp, collect_timestamp)
    print('소요시간:', time.time() - start)