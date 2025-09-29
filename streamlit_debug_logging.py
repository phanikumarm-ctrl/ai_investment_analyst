import streamlit as st
import logging

# Configure logging for your application
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

st.title("My Debugging App")

logging.debug("This is a debug message from my app.")
st.write("Hello, Streamlit!")
logging.info("An informational message after st.write.")

if st.button("Click me"):
    try:
        result = 1 / 0 # Example of an error
        st.write(f"Result: {result}")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        st.error("An error occurred. Check the console for details.")
