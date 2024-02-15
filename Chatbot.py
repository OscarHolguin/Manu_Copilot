# Databricks notebook source
import streamlit as st
from langchain.agents.format_scratchpad import format_to_openai_functions
from langchain.agents.output_parsers import OpenAIFunctionsAgentOutputParser
from pydantic import BaseModel
import time
import nltk

from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.tools.render import format_tool_to_openai_function


eygif = "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExNmpycDlnZjhpMHJqOTc2NjhlMms0eTBwazN4eHc1OG02amZkZ3g4ZyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9cw/eKleRTvdp0qXR8l3e7/giphy.gif"
st.set_page_config(page_title = "Copilot-RCA",
                   page_icon =eygif,#"https://upload.wikimedia.org/wikipedia/commons/thumb/3/34/EY_logo_2019.svg/1024px-EY_logo_2019.svg.png",
                   layout="wide",
                   initial_sidebar_state="expanded", 
    )

#st.image("https://tse4.mm.bing.net/th/id/OIP.nxkAddPMpqnAlVZN-YhIBwHaHw?rs=1&pid=ImgDetMain",width=150)
# if not st.session_state.get("messages"):
#     st.image("https://th.bing.com/th/id/R.2f543723e0d97263a5d4b07a48179fbf?rik=wIY0H2lT3OHXUQ&riu=http%3a%2f%2fwww.aislingsinclair.com%2fwp-content%2fuploads%2f2020%2f03%2fFeatured-Image-LARGE-300.gif&ehk=AwrXRF6v%2bPujkguHiN1sR1QTE%2ftJO1a3KpxkKpjXB2o%3d&risl=&pid=ImgRaw&r=0",
#          width = 150)
# st.image("https://media0.giphy.com/media/l10Snuqy6sZ8O1Eq8L/200.webp",
#          width = 150)


hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)

#keep buttons fixated
style = """
<style>
    #stButtonVoice {
        position: fixed;
        bottom: 0;
        right: 0;
        margin-left: 1rem;
    }
    .stTextInput {
        position: fixed;
        bottom: 0;
        left: 0;
        margin-right: 1rem;
    }
</style>
"""

# Inject the styling code for both elements
st.markdown(style, unsafe_allow_html=True)


############# Functions


st.title("Copilot-RCA")

st.image("https://th.bing.com/th/id/R.2f543723e0d97263a5d4b07a48179fbf?rik=wIY0H2lT3OHXUQ&riu=http%3a%2f%2fwww.aislingsinclair.com%2fwp-content%2fuploads%2f2020%2f03%2fFeatured-Image-LARGE-300.gif&ehk=AwrXRF6v%2bPujkguHiN1sR1QTE%2ftJO1a3KpxkKpjXB2o%3d&risl=&pid=ImgRaw&r=0",
         width = 150)

@st.cache_resource()
def importinits():
    from ResponsePipeline import vecrag,llmrca,loaded_embeddings,rag2
    return vecrag,llmrca,loaded_embeddings,rag2
    #my_bar.empty()


vecrag,llmrca,loaded_embeddings,rag2 = importinits()
vecragsug = rag2(llmrca,None,type="langchain",embeddings = loaded_embeddings,usememory=True)


@st.cache_resource
def get_copilot_response(prompt,**kwargs):
    #VECTOR RAG ONLY FOR NOW
    
    response =  vecrag.response(query=prompt,method="vector",store="faiss",text_embeddings = loaded_embeddings)#vectorchain({"question": prompt}).get("answer")
    return response


#generate stream like response
def stream_response(response,speech=False):
    progress_text = "Operation in progress. Please wait."
    my_bar = st.sidebar.progress(0, text=progress_text)
    for n,word in enumerate(response):
        yield word +""
        my_bar.progress(n/len(response)) #progress bar
        time.sleep(0.02)
    if speech:
        speech_response(response)

    my_bar.empty()


#STOP AND RUN BUTTONS
if "app_stopped" not in st.session_state:
    st.session_state["app_stopped"] = False 
elif st.session_state["app_stopped"]:
    st.session_state["app_stopped"] = False


def Running():
    with st.spinner("running"):
        time.sleep(60)

def stopRunning():
    try:
        st.session_state["app_stopped"] = True
    except:
        pass
def reset_conversation():
    try:
        st.session_state.conversation = None
        st.session_state.chat_history = None
        vecrag,llmrca,loaded_embeddings,rag2 = importinits()
        st.session_state.messages = [{"role": "assistant", "content": "Hi this is your Copilot! How can I help you?"}]

        #st.cache_data.clear
        st.empty()
    except:
        pass
    #st.session_state.messages = None

def continue_generating():
    st.session_state.messages.append({"role": "user", "content": "Continue Generating"})
    with st.chat_message("assistant"):
        response =  stream_response(get_copilot_response("Continue Generating"))
        message = {"role": "assistant", "content": response}
        st.session_state.messages.append(message) 

if st.session_state["app_stopped"]:
    print("stopping this NOW")
    st.stop()
    
#SPEECH RECOGNITION PART

import speech_recognition as sr

def transcribe_speech():
    r = sr.Recognizer()
    
    #with audio as source:
    with sr.Microphone(device_index=0) as source:
        r.adjust_for_ambient_noise(source)
        #with st.chat_message("assistant"):
        with st.spinner("Listening..."):
                audio = r.listen(source)
                with st.spinner("Transcribing..."):
                    try:
                        text = r.recognize_google(audio)
                        return text
                    except sr.UnknownValueError:
                        st.write("Could not understand audio")
                    except sr.RequestError as e:
                        st.write(f"Could not request results from Google Speech Recognition service; {e}")
                
                
#sr button
from streamlit_extras.stylable_container import stylable_container

            

#st.button("run", on_click=Running)
@st.cache_resource()
def generate_suggestions():
    "Generates suggestions using vector RAG context "
    prompt = "based on the context retrieved information generate 5 questions, 3 of them should be about the incident tickets  and the rest of the context information you have"
    rsugs = vecragsug.response(query=prompt,method="vector",store="faiss",text_embeddings = loaded_embeddings)
    return rsugs
    #return get_copilot_response(prompt)#,method="vector",store="faiss",text_embeddings = loaded_embeddings)

#@st.cache_resource()
def speech_response(text):
    import pyttsx3
    engine = pyttsx3.init()
    engine.say(text)
    engine.runAndWait()




############################################################################################################



button_col,text_col = st.columns([4, 1])


speechreply = False
#################################################CHAT PART###################################################
if True:
         #EY Logo

         st.sidebar.image("https://tse4.mm.bing.net/th/id/OIP.hRdpi21W-qf50yPPNKsrwQHaHy?rs=1&pid=ImgDetMain",width=100)
         
         col1, col2, col3  = st.sidebar.columns(3)

         #STOP
         col1.button("Stop 🛑",on_click=stopRunning)
         
         #Continue running
         col2.button("Run ▶️", on_click=Running)

         
         #RESET CONVERSATION (CLEAR IT)  
         col3.button('Reset', on_click=reset_conversation)
         
         #Continue generating
         #st.sidebar.button("Continue Generating",on_click = continue_generating)
         
        #  #bar definition
        #  progress_text = "Operation in progress. Please wait."
        #  my_bar = st.sidebar.progress(0, text=progress_text)
        
        
         st.session_state.prompt_history = []
         if st.sidebar.toggle("Suggest insights :bulb:"):
             st.sidebar.subheader("Suggested Inisghts:")
             suggestions = generate_suggestions()
             copilot_suggestions = [n for n in nltk.sent_tokenize(' '.join([x for x in nltk.word_tokenize(suggestions)]))]
             copilot_suggestions = [x for x in copilot_suggestions if x not in [str(n)+' .' for n in list(range(1,6))]]
             pclickables = {}
    
             with st.sidebar: 
                 for psug in copilot_suggestions:
                     pclickables[psug] = st.button(psug + " ➤ ", type="secondary")
                     if pclickables[psug]:
    
                           st.session_state.messages.append({"role": "user", "content": psug})
                           with st.chat_message("assistant"):
                                   response =  stream_response(get_copilot_response(psug))
                                   message = {"role": "assistant", "content": response}
                                   st.session_state.messages.append(message) 

         #check speech recognition part
         with stylable_container(key="stButtonVoice",css_styles="""
            {
                position: fixed;
                bottom: 120px;
                
            }
            """):
            # ready_button = st.button("🎙️", key='stButtonVoice')
            #  ##      
            # if ready_button:
            #     with st.spinner("Loading STT"):
            #         from streamlit_mic_recorder import mic_recorder,speech_to_text

            #         audio=mic_recorder(start_prompt="⏺️",stop_prompt="⏹️",key='recorder')
            #         #if audio:
            #         text = transcribe_speech()
            #     if text:
            #         #append messages                         
            #         st.session_state.messages.append({"role": "user", "content": text})
            #         st.sidebar.write(f"Last thing you said: {text}")
            #     # with st.chat_message("assistant"):
            #         response_listen = get_copilot_response(text)
            #         response_audio =  stream_response(response_listen,speech=True) #response with speaking 
            #         message_audio = {"role": "assistant", "content": response_audio}
            #         st.session_state.messages.append(message_audio)
             

                    
                    

         if "messages" in st.session_state: 
             print('messages found')
             
         else:
            if "messages" not in st.session_state.keys():
                st.session_state.messages = [{"role": "assistant", "content": "Hi this is Copilot! How can I help you?"}]
                                         
        
         if "messages" in st.session_state:

             ##
             for message in st.session_state.messages:
                 with st.chat_message(message["role"]):
                     #st.write_stream(stream_response(message["content"])) #srteaming response
                     st.write(message["content"]) #write non stream

                     
             if prompt := st.chat_input():
                 st.session_state.messages.append({"role": "user", "content": prompt})
                 with st.chat_message("user"):
                         st.write(prompt)
            
            
             if st.session_state.messages[-1]["role"] != "assistant":
                if prompt:
                     with st.chat_message("assistant"):
                         with st.spinner("Thinking..."):
                             response =  get_copilot_response(prompt)
                             st.write_stream(stream_response(response))
                             message = {"role": "assistant", "content": response}
                             if speechreply:
                                print("Its getting through hereeeeeeeeeeeeeeeeeeeeeeee")
                                st.write(message["content"])
                                #speech_response(message["content"])
                                speechreply  =False
                             st.session_state.messages.append(message) 

#st.button("Continue Generating",on_click = continue_generating)

# from streamlit_mic_recorder import mic_recorder,speech_to_text
# state=st.session_state
# if 'text_received' not in state:
#     state.text_received=[]
    
    
# with st.sidebar:
#     st.write("Convert speech to text:")
#     text=speech_to_text(language='en',use_container_width=True,just_once=True,key='STT')
#     if text:       
#         state.text_received.append(text)
    
#     for text in state.text_received:
#         st.text(text)
        
#     st.write("Record your voice, and play the recorded audio:")
#     audio=mic_recorder(start_prompt="⏺️",stop_prompt="⏹️",key='recorder2')

#     if audio:
#         st.audio(audio['bytes'])
    
    # audio=mic_recorder(
    #     start_prompt="Start recording",
    #     stop_prompt="Stop recording", 
    #     just_once=False,
    #     use_container_width=False,
    #     callback=None,
    #     args=(),
    #     kwargs={},
    #     key=None
    # )


# from streamlit_extras.stylable_container import stylable_container
# from st_audiorec import st_audiorec

# wav_audio_data = st_audiorec()

# if wav_audio_data is not None:
#     st.audio(wav_audio_data, format='audio/wav')

# with stylable_container(
#         key="bottom_content",
#         css_styles="""
#             {
#                 position: fixed;
#                 bottom: 120px;
#             }
#             """,
#     ):
#         # audio = audiorecorder("🎙️ start", "🎙️ stop")
#         # print('audio: ', audio)
#         # if len(audio) > 0:
#         #     audio.export("audio.mp3", format="mp3")
#         pass

# st.chat_input("These are words.")

# with stylable_container(
#         key="text_input1",
#         css_styles="""
#             {
#                 position: fixed;
#                 bottom: 200px;
#             }
#             """,
#     ):
#     st.text_input(label = 'text' ,value = "These are words.")
    
    



# if st.button("Toggle mic input"):
#         if st.session_state.mic_toggle:
#             #process_input_message(st.session_state.mic_prompt, insert_sample)
#             st.session_state.mic_prompt = ""

#         st.session_state.mic_toggle = not(st.session_state.mic_toggle)

#     # Show device list of microphones in streamlit
#     selected_mic = st.selectbox("Select device", 1,2,3)#sr.Microphone.list_microphone_names())
#     #selected_mic_index = sr.Microphone.list_microphone_names().index(selected_mic)