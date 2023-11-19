import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--video_id", required=True)
args = parser.parse_args()

vid = args.video_id

import numpy as np
from transformers import WhisperProcessor, WhisperForConditionalGeneration
from scipy.io import wavfile
import re

if __name__ == '__main__':
	# load model and processor
	processor = WhisperProcessor.from_pretrained("openai/whisper-large-v2")
	model = WhisperForConditionalGeneration.from_pretrained("openai/whisper-large-v2")
	model.config.forced_decoder_ids = None
	
	
	samplerate, data = wavfile.read(f'/root/audio/{vid}.wav')
	data = data.astype(float).mean(axis=1)/32767

	data_split = np.array_split(data, data.shape[0]//(16000*20))

	transcription_output = []

	for inp in data_split:
		input_features = processor(inp, sampling_rate=samplerate, return_tensors="pt").input_features 

		# generate token ids
		predicted_ids = model.generate(input_features)
		# decode token ids to text
		# transcription = processor.batch_decode(predicted_ids, skip_special_tokens=False)

		transcription = processor.batch_decode(predicted_ids, skip_special_tokens=True)
		
		transcription_output.append(transcription)

	transcription_output = [x[0] for x in transcription_output]


			
	timestamp = 0
	transcript_output_timestamps = []
	for part in transcription_output[:]:

		part = re.sub('\syou$','', part)
			# remove music notes
		part = re.sub("�|♪|បា|នា|ន",'', part)

		timestamps_exclude_last = re.sub('(\.\s)', r'\1' + f'<|{timestamp}|> ', part)

		transcript_output_timestamps.append(timestamps_exclude_last)

		timestamp += 20
		
	transcript_output_p = ''.join(transcript_output_timestamps).strip() + f' <|{timestamp}|> '

	transcript_output_p = re.sub(r'(\.)(\s+[a-z].*?\.)', r', \2', transcript_output_p)


	with open(f'/root/data/raw/{vid}.txt', 'w') as fh:
		fh.write(transcript_output_p)