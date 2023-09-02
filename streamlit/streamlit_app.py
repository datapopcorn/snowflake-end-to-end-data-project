import streamlit as st
from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from datetime import datetime
import json
import pandas as pd
from time import sleep

st.title('Welcome to My Bookstore!')
# st.markdown(
#         """
#         <style>

#         </style>

#         """
#         ,
#         unsafe_allow_html=True,
# )



with open('creds.json') as f:
    connection_parameters = json.load(f)  
session = Session.builder.configs(connection_parameters).create()


def empty():
    ph.empty()
    sleep(0.01)

# Disable the submit button after it is clicked
def disable():
    st.session_state.disabled = True

# Login
def login():
    st.session_state.login = True
    st.session_state.disabled = False

# Signup
def signup():
    st.session_state.signup = True
    st.session_state.disabled = False

# Purchase
def purchase():
    st.session_state.purchase = True
    st.session_state.disabled = False


# Check if the user is signed up
def check_if_user_signup(user_email, password):
    # Check if user_email exists in the USER_EVENT table
    user_email = user_email
    password = password
    df = session.table("USER_EVENT")
    df = df.filter(f"USER_EMAIL = '{user_email}'")
    if df.count() == 0:
        return "User does not exist"
    else:
        #  Check if the password matches
        #  get the record with the latest login time
        df = df.filter("EVENT_TYPE = 'signup'").orderBy("EVENT_TIMESTAMP", ascending=False).limit(1)
        pddf = df.select("EVENT_JSON").toPandas()
        password_db = json.loads(pddf.iloc[0]["EVENT_JSON"].replace("'", '"'))["password"]
        if password_db == password:
            return "User exists and password matches"
        else:
            return "User exists but password does not match"



# Initialize disabled for form_submit_button to False

if "login" not in st.session_state:
    st.session_state.login = False

if "signup" not in st.session_state:
    st.session_state.signup = False

if "disabled" not in st.session_state:
    st.session_state.disabled = False

if "purchase" not in st.session_state:
    st.session_state.purchase = False

ph = st.empty()

with ph.container():
    st.write("Please login to continue")
    st.button("Login", on_click=login)
    st.write("Don't have an account?")
    st.button("Sign Up", on_click=signup)

if st.session_state.signup:
    empty()
    with ph.container():
        with st.form(key='signup_form'):
            st.header('Sign Up')
            st.write("Please fill in the details below")
            user_name = st.text_input(label='Name')
            user_email = st.text_input(label='Email')
            password = st.text_input(label='Password', type='password')
            phone_number = st.text_input(label='Phone Number')
            signup_submitted = st.form_submit_button(label='Submit', on_click=disable, disabled=st.session_state.disabled)
        if signup_submitted:
            event_type = 'signup'
            signup_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            signup_data = f"""{{"user_name": "{user_name}", "user_email": "{user_email}", "password": "{password}", "phone_number": "{phone_number}", "signup_timestamp": "{signup_timestamp}"}}"""

            signup_event = {'event_type': event_type,
                        'user_email': user_email,
                        'event_timestamp': signup_timestamp,
                        'event_json': signup_data}
            
            session.sql(f"INSERT INTO USER_EVENT (event_type, user_email, event_timestamp, event_json) VALUES ('{event_type}', '{user_email}', '{signup_timestamp}', '{signup_data}')").collect()
            st.success('Data successfully submitted!!')
            login()
            st.button("Let's Buy Some Books!", on_click=purchase)

if st.session_state.login & ~st.session_state.signup:
    empty()
    with ph.container():
        with st.form(key='login_form'):
            st.header('Login')
            user_email = st.text_input(label='Email')
            password = st.text_input(label='Password', type='password')
            login_submitted = st.form_submit_button(label='Submit', on_click=disable, disabled=st.session_state.disabled)
        if login_submitted:
            if check_if_user_signup(user_email, password) == "User does not exist":
                st.error("User does not exist. Please sign up first")
                st.button(label="Sign Up", key="signupfirst", on_click=signup)
            elif check_if_user_signup(user_email, password) == "User exists but password does not match":
                st.error("Password does not match. Please try again")
            else:
                event_type = 'login'
                login_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                login_data = f"""{{"user_email": "{user_email}", "login_timestamp": "{login_timestamp}"}}"""
                # Create a dictionary
                login_event = {'event_type': event_type,
                            'user_email': user_email,
                            'event_timestamp': login_timestamp,
                            'event_json': login_data}

                session.sql(f"INSERT INTO USER_EVENT (event_type, user_email, event_timestamp, event_json) VALUES ('{event_type}', '{user_email}', '{login_timestamp}', '{login_data}')").collect()
                st.success('Data successfully submitted!!')
                st.button("Let's Buy Some Books!", on_click=purchase)

if st.session_state.purchase:
    empty()
    with ph.container():

        with st.form(key='purchase_form', clear_on_submit=True):
                
                # Say Hi to the user
                st.header("Hi {}! What do you want to buy today?".format(user_email))
                # create a checklist for products to buy ["Harry Potter", "Lord of the Rings", "The Alchemist"]
                product_name = st.selectbox(label='Product', options=["Harry Potter", "Lord of the Rings", "The Alchemist"])
                quantity = st.number_input(label='Quantity', min_value=1, max_value=10, step=1)
                purchase_submitted = st.form_submit_button(label='Purchase', on_click=disable, disabled= st.session_state.disabled)
                
        if purchase_submitted:
                
                event_type = 'purchase'
                purchase_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                purchase_data = f"""{{"user_email": "{user_email}", "product_name": "{product_name}", "quantity": "{quantity}", "purchase_timestamp": "{purchase_timestamp}"}}"""
                
                purchase_event = {'event_type': 'purchase',
                                'user_email': user_email,
                                'event_timestamp': purchase_timestamp,
                                'event_json': purchase_data}

                session.sql(f"INSERT INTO USER_EVENT (event_type, user_email, event_timestamp, event_json) VALUES ('{event_type}', '{user_email}', '{purchase_timestamp}', '{purchase_data}')").collect()
                st.success("You have bought {} {} books at {}".format(quantity, product_name, purchase_timestamp))
                st.button("Buy More Books", on_click=purchase)