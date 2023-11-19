import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--video_id", required=True)
parser.add_argument("--channel_id", required=True)
args = parser.parse_args()

vid = args.video_id
cid = args.channel_id

import re
from transformers import AutoTokenizer, AutoModel
import torch
import torch.nn.functional as F
from torch import nn
import numpy as np
import json
import pandas as pd

#Mean Pooling - Take attention mask into account for correct averaging
def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0] #First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

# For some reason the token size is not the same, are there start/end of line tokens? -3?
def get_token_list(inp_list):

    tokens = []
    token_len = []
    for inp in inp_list:
        tmp = tokenizer(inp, max_length=1024, return_tensors="pt")['input_ids']
        tokens.append(tmp)
        token_len.append(tmp.shape[1])
        
    return tokens, token_len

def get_token_len(joined_input):
    tmp = tokenizer(joined_input, max_length=1024, return_tensors="pt")['input_ids']
    token_cnt = tmp.shape[1]
    return token_cnt

def get_next_n_tokens(input_split, indx, n):
    
    copy_indx = indx
    
    token_cnt = 0.0
    while indx < len(input_split) and token_cnt < n:
        tmp = ' '.join(input_split[copy_indx:indx+1])
        tmp = tokenizer(tmp, max_length=1024, return_tensors="pt")['input_ids']
        token_cnt = tmp.shape[1]
        indx += 1

    reached_token_len = False
    if token_cnt >= n:
        reached_token_len = True
        
    return_indx = indx
    
    return return_indx, token_cnt, reached_token_len

def compute_similarities(s1, s2):
    
    sentences = [s1, s2]
    encoded_input = tokenizer(sentences, padding=True, truncation=True, return_tensors='pt')

    # Compute token embeddings
    with torch.no_grad():
        model_output = model(**encoded_input)

    # Perform pooling
    sentence_embeddings = mean_pooling(model_output, encoded_input['attention_mask'])

    # Normalize embeddings
    sentence_embeddings = F.normalize(sentence_embeddings, p=2, dim=1)
    
    sim = cossim(sentence_embeddings[0:1],sentence_embeddings[1:2])
    
    return sim

def check_bool_true_next_n_chunks(vals, indx, n):

    return ~np.any(vals[indx:indx+n] == 1)

def return_prediction_at_cutoff(predictions, cutoff):
    raw = []
    preds = []
    for x in predictions:
        preds.append((x>cutoff).astype(int))
        raw.append(x)
    preds = np.array(preds)
    return preds

def get_paragraphs_at_cutoff(preds, transcript_sentences, transcript_timestamps):
    
    final_paragraphs = []
    final_timestamps = []
    join_values = False
    last_indx = 0
    for indx in range(1, preds.shape[0]): 
        chunk_bool = check_bool_true_next_n_chunks(preds, indx, 1)

        if chunk_bool == False and join_values == False and (indx-last_indx)>1:

            join_txt = ' '.join(transcript_sentences[last_indx:indx])

            min_timestamp = min(transcript_timestamps[last_indx:indx])

            if len(final_timestamps)>0 and final_timestamps[-1] == min_timestamp:
                min_timestamp += 10

            final_paragraphs.append(join_txt)
            final_timestamps.append(min_timestamp)

            join_values = True
            last_indx = indx

        if chunk_bool == True:

            join_values = False

    join_txt = ' '.join(transcript_sentences[last_indx:])

    final_paragraphs.append(join_txt)
    final_timestamps.append(min(transcript_timestamps[last_indx:]))
    
    return final_paragraphs, final_timestamps



if __name__ == '__main__':
    
    cossim = nn.CosineSimilarity()

    with open(f'/root/data/raw/{vid}.txt', 'r') as fh:
        transcript_output_p = fh.read()

    transcript_parse_output = re.findall(r'(.*?) <\|(\d+)\|> ',transcript_output_p)

    transcript_sentences = []
    transcript_timestamps = []
    for sentence, timestamp in transcript_parse_output:
        
        if len(sentence.strip())>0:
            transcript_sentences.append(sentence)
            transcript_timestamps.append(int(timestamp))
        

    # Load model from HuggingFace Hub
    tokenizer = AutoTokenizer.from_pretrained('sentence-transformers/all-MiniLM-L6-v2')
    model = AutoModel.from_pretrained('sentence-transformers/all-MiniLM-L6-v2')

    corpus_split = transcript_sentences

    corpus_split_len = len(transcript_sentences)

    collect_n_tokens = 133
    token_buffer = 87
    result = []
    min_timestamp = 0

    for start_split_indx in range(corpus_split_len):

        start_token_indx, start_token_cnt, start_reached_token_len = get_next_n_tokens(corpus_split, start_split_indx, collect_n_tokens)
        buffer_token_indx, buffer_token_cnt, buffer_reached_token_len = get_next_n_tokens(corpus_split, start_split_indx, collect_n_tokens+token_buffer)
        
        if start_reached_token_len == True and (buffer_token_cnt-start_token_cnt) >= 20:
                
                
                start_tokens_input = ' '.join(corpus_split[start_split_indx:start_token_indx+1])
                start_tokens_input = tokenizer(start_tokens_input, max_length=1024, return_tensors="pt")['input_ids']
                                            

                end_tokens_input = ' '.join(corpus_split[start_token_indx+1:buffer_token_indx+1])
                end_tokens_input = tokenizer(end_tokens_input, max_length=1024, return_tensors="pt")['input_ids']   
                                            
    #             print(start_tokens_input.shape, end_tokens_input.shape, buffer_token_cnt, start_token_indx)
                input_paragraph = tokenizer.batch_decode(start_tokens_input, skip_special_tokens=True, clean_up_tokenization_spaces=False)[0]
                
                output_paragraph = tokenizer.batch_decode(end_tokens_input, skip_special_tokens=True, clean_up_tokenization_spaces=False)[0]


                
                result.append((input_paragraph, output_paragraph, start_token_indx))



    predictions = []
    indexes = []

    for s1, s2, stindx in result:
        # Tokenize sentences
        
        sim = compute_similarities(s1, s2).detach().numpy()
        
        predictions.append(sim)
        indexes.append(stindx)


    cutoff_analysis = {}

    for cutoff in np.linspace(0.5, 0.99, 10):
        
        cutoff = round(cutoff, 2)
        preds = return_prediction_at_cutoff(predictions, cutoff)

        final_paragraphs, final_timestamps = get_paragraphs_at_cutoff(preds, transcript_sentences, transcript_timestamps)

        print('cutoff',cutoff)
        token_lens = []
        for paragraph in final_paragraphs:
            token_lens.append(get_token_len(paragraph))
            print(get_token_len(paragraph))
            
        cutoff_analysis[cutoff] = token_lens
            
    max_token_len = 725

    best_cutoff = 0.5
    best_min_size = 0
    for cutoff in cutoff_analysis:
        min_size = min(cutoff_analysis[cutoff])
        max_size = max(cutoff_analysis[cutoff])
        
        if min_size >= best_min_size and max_size <= max_token_len:
            best_min_size = min_size
            best_cutoff = cutoff
            
    preds = return_prediction_at_cutoff(predictions, best_cutoff)

    final_paragraphs, final_timestamps = get_paragraphs_at_cutoff(preds, transcript_sentences, transcript_timestamps)

    df = pd.DataFrame()
    df['timestamp'] = final_timestamps
    df['text'] = final_paragraphs
    df['video_id'] = vid
    df['channel_id'] = cid

    df.to_csv(f'/root/data/video_transcript/{vid}.csv', index=False)