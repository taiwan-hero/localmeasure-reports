from datetime import datetime
import re

stop_words = set(["a's", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "ain't", 
    "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", 
    "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "aren't", "around", 
    "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", 
    "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", 
    "c'mon", "c's", "came", "can", "can't", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", 
    "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldn't", "course", "currently", 
    "definitely", "described", "despite", "did", "didn't", "different", "do", "does", "doesn't", "doing", "don't", "done", "down", "downwards", "during", 
    "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", 
    "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "fifth", "first", "five", "followed", "following", "follows", 
    "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", 
    "got", "gotten", "greetings", "had", "hadn't", "happens", "hardly", "has", "hasn't", "have", "haven't", "having", "he", "he's", "hello", "help", "hence", 
    "her", "here", "here's", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", 
    "however", "i'd", "i'll", "i'm", "i've", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", 
    "insofar", "instead", "into", "inward", "is", "isn't", "it", "it'd", "it'll", "it's", "its", "itself", "just", "keep", "keeps", "kept", "know", "known", "knows", 
    "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "let's", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", 
    "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", 
    "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally",
     "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", 
     "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", 
     "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", 
     "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming",
      "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldn't", "since", "six", "so", "some", 
      "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", 
      "such", "sup", "sure", "t's", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "that's", "thats", "the", "their", "theirs", "them", 
      "themselves", "then", "thence", "there", "there's", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "they'd", "they'll", 
      "they're", "they've", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", 
      "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", 
      "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasn't", 
      "way", "we", "we'd", "we'll", "we're", "we've", "welcome", "well", "went", "were", "weren't", "what", "what's", "whatever", "when", "whence", "whenever", "where",
       "where's", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "who's", "whoever", "whole", "whom",
        "whose", "why", "will", "willing", "wish", "with", "within", "without", "won't", "wonder", "would", "wouldn't", "yes", "yet", "you", "you'd", "you'll", "you're", 
        "you've", "your", "yours", "yourself", "yourselves", "zero"])

@outputSchema('is_expired:int')
def is_expired(expires_at):
    #Tue Dec 22 12:05:21 EST 2015
    #exp = datetime.strptime(str(expires_at), "%a %b %d %H:%M:%S %Z %Y")

    exp = datetime.fromtimestamp(expires_at/1000)
    now = datetime.now()

    if now > exp:
        return 1
    else:
        return 0

#removes [] chars from venue_id array
@outputSchema('venue_ids:chararray')
def venue_id_strip(mongo_venue_id_str):
    start = mongo_venue_id_str.find('[')
    end = mongo_venue_id_str.find(']')
    if start == -1:
        start = 0
    if end == -1:
        end = len(mongo_venue_id_str)
    return mongo_venue_id_str[start+1:end]

#remove stopwords, punctuation, #hashtags, @mentions from post text
#returns space separated string of relevant words
@outputSchema('text:chararray')
def text_strip(mongo_post_text):
    output_text = ''
    for word in mongo_post_text.split(" "):
        word = word.lower()
        if len(word) < 3:
            continue
        if len(word) > 16:
            continue
        if word.find('http') != -1:
            continue
        if word[0] == '#' or word[0] == '@':
            word = word[1:]
        if word in stop_words:
            continue
        if len(output_text) == 0:
            output_text = word
        else:
            output_text = output_text + ' ' + word

    return output_text

@outputSchema('month:chararray')
def get_month(mongo_date):
    month = datetime.fromtimestamp(mongo_date/1000)

    return month.strftime('%Y%m')

@outputSchema('output_bag_field_name:bag{t:(inner_field_name_1:chararray, inner_field_name_2:int)}')
def format_output(arg0):
    bag = []
    # each element in the bag must be a tuple
    bag.append(('FB', 1000))
    bag.append(('IG', 500))
    bag.append(('TW', 100))
    bag.append(('4S', 50))

    return bag

@outputSchema('counts:map[]')
def map_output(arg0):
    fb_count = 0
    ig_count = 0
    tw_count = 0
    _4s_count = 0

    for elem in arg0:
        if elem[4] == 'FB':
            fb_count = int(elem[3])
        elif elem[4] == 'IG':
            ig_count = int(elem[3])
        elif elem[4] == 'TW':
            tw_count = int(elem[3])
        elif elem[4] == '4S':
            _4s_count = int(elem[3])

    return {'FB': fb_count, 'IG': ig_count, 'TW': tw_count, '4S': _4s_count}

#FIXME: make this work - currently doesnt
@outputSchema('object_id:bytearray')
def to_object_id(arg):
    return arg










