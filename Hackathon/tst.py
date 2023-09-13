import streamlit as st
import toml
import pandas as pd
import snowflake.connector as sfc


menu = ["Home","Connect"]
choice = st.sidebar.selectbox("Menu",menu)

sidebar1=''
sidebar2=''

#SNOWFLAKE CONNECTION FUNCTION

def connect_to_snowflake(account,username,password,role,database,warehouse):
    cnt = sfc.connect(account=account,user=username,password=password,role=role\
                      ,database=database,warehouse=warehouse)
    cs = cnt.cursor()
    st.session_state['conn'] = cs
    st.session_state['is_ready']= True
    return cs

#MENU ACTIONS

if choice == "Connect":
    st.subheader("Login")
    #sidebar1 = st.sidebar
    account = st.text_input("Account")
    username = st.text_input("User Name")
    password = st.text_input("Password",type='password')
    role = st.text_input("Role")
    database = st.text_input("Database")
    schema = st.text_input("Schema")
    warehouse = st.text_input("Warehouse")
    connect = st.button("Connect to Snowflake",on_click = connect_to_snowflake\
                        ,args = [account,username,password,role,database,warehouse])
elif choice == "Home":
    #sidebar2 = st.sidebar
    #with sidebar2:
    database = st.text_input("Databases")

#sidebar = st.sidebar

def excute_query():
    rs = st.session_state['conn'].execute("select * from STMSNW.BASE_SCH.oxford")
    rs = st.session_state['conn'].fetch_pandas_all()
    return rs



if 'is_ready' not in st.session_state:
    st.text("Try Again")
    st.session_state['is_ready'] = False


if st.session_state['is_ready'] == True:
    #st.write("connected")
    st.text("Successful")
    data =  excute_query()
    st.dataframe(data)


