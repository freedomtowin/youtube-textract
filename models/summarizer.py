import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--video_id", required=True)
parser.add_argument("--channel_id", required=True)
args = parser.parse_args()

vid = args.video_id
cid = args.channel_id

import json
from transformers import AutoTokenizer, BartForConditionalGeneration
import pandas as pd
import re


first_letter_pattern = re.compile(r'(?<=[\.\?!]\s)(\w+)')
def capitalize(match):
    return(match.group().capitalize())

if __name__ == '__main__':

    model = BartForConditionalGeneration.from_pretrained("facebook/bart-large-cnn")
    tokenizer = AutoTokenizer.from_pretrained("facebook/bart-large-cnn")

    df = pd.read_csv(f'/root/data/video_transcript/{vid}.csv')

    timestamps = df['timestamp']
    paragraphs = df['text']

    result = []
    for sentences in paragraphs:

        inputs = tokenizer([sentences], max_length=1024, return_tensors="pt")
        input_size = inputs["input_ids"].shape[1]

        # Generate Summary
        summary_ids = model.generate(inputs["input_ids"], num_beams=20, do_sample = False, min_length=int(0.20*input_size), max_length=input_size+1)
        summary = tokenizer.batch_decode(summary_ids, skip_special_tokens=True, clean_up_tokenization_spaces=False)[0]
        summary_len = tokenizer([summary], max_length=1024, return_tensors="pt")["input_ids"].shape[1]
        
            
        summary = first_letter_pattern.sub(capitalize, summary)
        result.append(summary)

    df_summary = df.copy()
    df_summary['text'] = result
    df_summary.to_csv(f'/root/data/video_summary/{vid}.csv', index=False)

