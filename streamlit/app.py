import streamlit as st
import boto3
import re
import pandas as pd
import time

# AWS 설정
REGION = "ap-northeast-2"
agent_runtime = boto3.client("bedrock-agent-runtime", region_name=REGION)
athena = boto3.client("athena", region_name=REGION)

# 에이전트 설정
AGENT_ID = '<Agent ID>'
AGENT_ALIAS_ID = '<Agent Alias ID>'
SESSION_ID = '<Session ID>'

# Athena 설정
ATHENA_DB = "<Glue DB Name>"
ATHENA_OUTPUT = "s3://<Athena Result Bucket>/output/"

# 지식 기반 Agent 호출 함수: Bedrock Agent에 자연어 질문을 보내고 응답(설명+SQL)을 받는 함수
def query_knowledge_base(user_query):
    try:
        response_stream = agent_runtime.invoke_agent(
            agentId=AGENT_ID,
            agentAliasId=AGENT_ALIAS_ID,
            sessionId=SESSION_ID,
            inputText=user_query
        )
        full_response = ""
        for event in response_stream["completion"]:
            chunk = event.get("chunk")
            if chunk and "bytes" in chunk:
                full_response += chunk["bytes"].decode("utf-8")
        return full_response
    except Exception as e:
        st.error(f"Agent 오류: {e}")
        return ""

# Bedrock Agent 응답에서 SQL 코드블록만 추출하는 함수
def extract_sql(text):
    match = re.search(r"```sql\s*(.*?)```", text, re.IGNORECASE | re.DOTALL)
    if match:
        sql = match.group(1).strip()
        if not sql.endswith(';'):
            sql += ';'
        return sql
    return None

# Athena에 SQL 쿼리를 실행하고 결과를 반환하는 함수
def run_athena_query(sql):
    execution = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    execution_id = execution["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)

    if state == "SUCCEEDED":
        results = athena.get_query_results(QueryExecutionId=execution_id)
        return results
    else:
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "알 수 없는 에러")
        st.error(f"쿼리 실패 \n{reason}")
        return None

# Athena 결과를 DataFrame으로 변환
def athena_results_to_df(results):
    headers = [col["VarCharValue"] for col in results["ResultSet"]["Rows"][0]["Data"]]
    rows = [
        [col.get("VarCharValue", "") for col in row["Data"]]
        for row in results["ResultSet"]["Rows"][1:]
    ]
    return pd.DataFrame(rows, columns=headers)

# Streamlit 챗봇 UI
st.set_page_config(page_title="Text2SQL Chatbot")
st.title("Text2SQL Chatbot")

if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []


user_input = st.chat_input("자연어로 SQL을 물어보세요!")

if user_input:
    st.session_state.chat_history.append({"role": "user", "text": user_input})
    with st.spinner("Bedrock Agent로 SQL 생성 중..."):
        agent_response = query_knowledge_base(user_input)
        # Agent 전체 응답(설명+SQL) 먼저 출력
        st.session_state.chat_history.append({"role": "assistant", "text": agent_response})
        # SQL만 추출
        sql = extract_sql(agent_response)
    if sql:
        # Athena 쿼리 실행 및 결과 표시
        with st.spinner("Athena 쿼리 실행 중..."):
            results = run_athena_query(sql)
            if results:
                df = athena_results_to_df(results)
                st.session_state.chat_history.append({"role": "assistant", "text": "Athena 쿼리 결과입니다.", "dataframe": df})
            else:
                st.session_state.chat_history.append({"role": "assistant", "text": "Athena 쿼리 실패"})
    else:
        st.session_state.chat_history.append({"role": "assistant", "text": "다시 질문 해주세요!"})

for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["text"])
        if "dataframe" in message:
            st.dataframe(message["dataframe"])