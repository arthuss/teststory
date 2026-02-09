
def generate_atomic_ids(graphematic_string):
    """
    Parses a Ge'ez string into atomic characters with IDs and grouped words.
    
    Args:
        graphematic_string (str): The cleaned text string.
        
    Returns:
        tuple: (base_chars, words)
        - base_chars: List of dicts [{"id": 1, "char": "ቃ"}, ...]
        - words: List of dicts [{"word_id": 1, "text": "ቃለ", "char_ids": [1, 2]}, ...]
    """
    base_chars = []
    words = []
    
    if not graphematic_string:
        return base_chars, words

    current_word_char_ids = []
    current_word_text = []
    
    global_char_id = 1
    word_id = 1
    
    # Define separators. 
    # U+1361 (ETHIOPIC WORDSPACE) is '፡'
    # U+1362 (ETHIOPIC FULL STOP) is '።'
    # Extended punctuation might also be separators, but user specifically mentioned '፡'.
    # We will treat standard unicode whitespace and Ge'ez wordspace as delimiters.
    separators = set(['፡', '።', ' ', '\n', '\t', '\r', '፣', '፤', '፥', '፦', '፧', '፨'])

    for char in graphematic_string:
        # 1. Add to base_chars
        base_chars.append({"id": global_char_id, "char": char})
        
        # 2. Check if separator
        if char in separators:
            # If we have accumulated a word, finalize it
            if current_word_char_ids:
                word_text = "".join(current_word_text)
                words.append({
                    "word_id": word_id,
                    "text": word_text,
                    "char_ids": list(current_word_char_ids)
                })
                word_id += 1
                
                # Reset word buffers
                current_word_char_ids = []
                current_word_text = []
            
            # The separator itself is not added to any word (per user example).
        else:
            # It's a regular character, add to current word
            current_word_char_ids.append(global_char_id)
            current_word_text.append(char)
        
        global_char_id += 1
        
    # Handle any remaining word at the end of the string
    if current_word_char_ids:
        word_text = "".join(current_word_text)
        words.append({
            "word_id": word_id,
            "text": word_text,
            "char_ids": list(current_word_char_ids)
        })
        
    return base_chars, words
