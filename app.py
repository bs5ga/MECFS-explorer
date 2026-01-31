import os
import streamlit as st

st.title("ME/CFS + Long COVID Mechanism Explorer")
st.caption("Deployed on Railway ✅")

st.write("If you can see this, the web app is working.")
st.write("PORT:", os.getenv("PORT", "(not set locally)"))
st.write("DATABASE_URL present:", bool(os.getenv("DATABASE_URL")))
